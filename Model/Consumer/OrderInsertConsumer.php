<?php
namespace Klizer\Srs\Model\Consumer;

use Magento\Framework\MessageQueue\ConsumerInterface;
use Magento\Framework\MessageQueue\EnvelopeInterface;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\Framework\MessageQueue\QueueRepository;
use Magento\Framework\MessageQueue\Consumer\ConfigInterface as ConsumerConfig;
use Magento\Framework\MessageQueue\ConsumerConfigurationInterface;
use Psr\Log\LoggerInterface;
use Magento\Framework\App\ResourceConnection;

class OrderInsertConsumer implements ConsumerInterface
{
    protected $logger;
    protected $jsonHelper;
    protected $resource;
    protected $configuration;
    protected $consumerConfig;
    protected $queueRepository;
    protected $interval;
    protected $batchSize;

    public function __construct(
        LoggerInterface $logger,
        JsonHelper $jsonHelper,
        ResourceConnection $resource,
        ConsumerConfigurationInterface $configuration,
        ConsumerConfig $consumerConfig,
        QueueRepository $queueRepository,
        $interval = 5,
        $batchSize = 10
    ) {
        $this->logger = $logger;
        $this->jsonHelper = $jsonHelper;
        $this->resource = $resource;
        $this->configuration = $configuration;
        $this->consumerConfig = $consumerConfig;
        $this->queueRepository = $queueRepository;
        $this->interval = $interval;
        $this->batchSize = $batchSize;
    }

    public function process($maxNumberOfMessages = null)
    {
        // Log the start of the process
        $startTime = microtime(true);
        $this->logger->info("Starting Order Insert Consumer... " . $startTime);

        // Get queue and consumer configurations
        $queueName = $this->configuration->getQueueName();
        $consumerName = $this->configuration->getConsumerName();
        $connectionName = $this->consumerConfig->getConsumer($consumerName)->getConnection();

        // Get the queue repository instance
        $queue = $this->queueRepository->get($connectionName, $queueName);

        // Initialize message count
        $totalMessagesProcessed = 0;

        // Run either in daemon mode or with a specific number of messages
        if (!isset($maxNumberOfMessages)) {
            $totalMessagesProcessed = $this->runDaemonMode($queue);
        } else {
            $totalMessagesProcessed = $this->run($queue, $maxNumberOfMessages);
        }

        // Capture the end time
        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        // Log the total messages processed and the execution time
        $this->logger->info(
            "Order Insert Consumer completed. Total execution time: " . number_format($executionTime, 2) . " seconds. Processed {$totalMessagesProcessed} orders."
        );
    }

    private function runDaemonMode($queue)
    {
        $transactionCallback = $this->getTransactionCallback($queue);
        $messageCount = 0;

        $maxExecutionTime = 600; // 10 minutes  
        $startTime = time();      
        while (true) {
            if (time() - $startTime >= $maxExecutionTime) {
                $this->logger->info("Daemon mode reached max execution time, stopping.");
                break;
            }

            $messages = $this->batchSize > 0
                ? $this->getMessages($queue, $this->batchSize)
                : $this->getAllMessages($queue);

            $transactionCallback($messages);
            $messageCount += count($messages);
            sleep($this->interval);
        }

        // Log total messages processed and the execution time
        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;
        $this->logger->info(
            "Order Insert Consumer completed in Daemon mode. Total execution time: " . number_format($executionTime, 2) . " seconds. Processed {$messageCount} orders."
        );

        return $messageCount;
    }

    private function run($queue, $maxNumberOfMessages)
    {
        $count = $maxNumberOfMessages ?: 1;
        $transactionCallback = $this->getTransactionCallback($queue);
        $messageCount = 0;

        while ($count > 0) {
            $messages = $this->getMessages($queue, min($count, $this->batchSize));
            $transactionCallback($messages);
            $messageCount += count($messages);
            $count -= $this->batchSize;
        }

        // Log total messages processed and the execution time
        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;
        $this->logger->info(
            "Order Insert Consumer completed. Total execution time: " . number_format($executionTime, 2) . " seconds. Processed {$messageCount} orders."
        );

        return $messageCount; 
    }

    private function getTransactionCallback($queue)
    {
        return function (array $messages) use ($queue) {
            list($messages, $messagesToAcknowledge) = $this->lockMessages($messages);
            $decodedMessages = $this->decodeMessages($messages);
            $this->processMessages($queue, $messages, $messagesToAcknowledge);
        };
    }

    private function processMessages($queue, $messages, $messagesToAcknowledge)
    {
        $startTime = time();
        $totalProcessed = 0; // Variable to track the number of processed orders
        $maxRetryLimit = 3; // Maximum number of retry attempts

        foreach ($messagesToAcknowledge as $message) {
            try {
                // Decode the message body
                $orderDetails = json_decode($message->getBody(), true);
                $orderDetails = json_decode($orderDetails, true);

                // Check for retry count or initialize it in the message body
                $messageData = json_decode($message->getBody(), true);
                $retryCount = isset($messageData['retry_count']) ? $messageData['retry_count'] : 0;

                // Process the order data
                $this->insertOrUpdateOrderData($orderDetails);

                // Acknowledge the message after successful processing
                $queue->acknowledge($message);
                $totalProcessed++; // Increment on successful processing
                $this->logger->info("Order data for order ID: " . $orderDetails['orderHeader']['orderNumber'] . " processed successfully.");
            } catch (\Exception $e) {
                // Log the error
                $this->logger->error("Error processing order: " . $e->getMessage());

                // Check if the retry limit is reached
                if ($retryCount >= $maxRetryLimit) {
                    // Move the message to the Dead Letter Queue (DLQ)
                    $this->moveMessageToDLQ($queue, $message, $retryCount);
                } else {
                    // Increment the retry count and requeue the message
                    $retryCount++;
                    // Update the retry count in the message body
                    $messageData['retry_count'] = $retryCount;
                    $message->setBody(json_encode($messageData)); // Set the updated body with retry count
                    $queue->reject($message); // Rejecting the message (this will cause it to be requeued)
                    $this->logger->info("Order message for order ID: " . $orderDetails['orderHeader']['orderNumber'] . " requeued with retry count: {$retryCount}.");
                }
            }
        }

        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        // Log the number of orders processed and total execution time for the batch
        $this->logger->info(
            "Processed {$totalProcessed} orders in " . number_format($executionTime, 2) . " seconds."
        );
    }

    private function insertOrUpdateOrderData($orderDetails)
    {
        $connection = $this->resource->getConnection();
        $tableName = $this->resource->getTableName('srs_erp_orders');

        // Prepare order data
        $line_item_codes = json_encode(['items' => array_column($orderDetails['orderDetails'], 'itemCode')]);
        $line_item_details = json_encode(['items' => $orderDetails['orderDetails']]);

        $data = [
            'order_number' => $orderDetails['orderHeader']['orderNumber'],
            'customer_code' => $orderDetails['orderHeader']['customerCode'],
            'bill_to_name' => $orderDetails['orderHeader']['billToName'],
            'bill_to_address1' => $orderDetails['orderHeader']['billToAddress1'],
            'bill_to_city' => $orderDetails['orderHeader']['billToCity'],
            'bill_to_state' => $orderDetails['orderHeader']['billToState'],
            'bill_to_zip' => $orderDetails['orderHeader']['billToZip'],
            'branch_code' => $orderDetails['orderHeader']['branchCode'],
            'branch_name' => $orderDetails['orderHeader']['branchName'],
            'delivery_charges' => $orderDetails['orderHeader']['deliveryCharges'],
            'order_date' => $orderDetails['orderHeader']['orderDate'],
            'order_status' => $orderDetails['orderHeader']['orderStatus'],
            'order_total' => $orderDetails['orderHeader']['orderTotal'],
            'order_type' => $orderDetails['orderHeader']['orderType'],
            'sale_type' => $orderDetails['orderHeader']['saleType'],
            'ship_via_code' => $orderDetails['orderHeader']['shipViaCode'],
            'ship_to_name' => $orderDetails['orderHeader']['shipToName'],
            'ship_to_address1' => $orderDetails['orderHeader']['shipToAddress1'],
            'ship_to_city' => $orderDetails['orderHeader']['shipToCity'],
            'ship_to_state' => $orderDetails['orderHeader']['shipToState'],
            'ship_to_zip' => $orderDetails['orderHeader']['shipToZip'],
            'sub_total' => $orderDetails['orderHeader']['subTotal'],
            'tax' => $orderDetails['orderHeader']['tax'],
            'delivery_date' => $orderDetails['orderHeader']['deliveryDate'],
            'delivery_window' => $orderDetails['orderHeader']['deliveryWindow'],
            'ship_complete' => $orderDetails['orderHeader']['shipComplete'] ? 1 : 0,
            'ordered_by' => $orderDetails['orderHeader']['orderedBy'],
            'line_item_codes' => $line_item_codes,
            'line_item_details' => $line_item_details
        ];
        $connection->insertOnDuplicate($tableName, $data, array_keys($data));
    }

    private function getAllMessages($queue)
    {
        $messages = [];
        while ($message = $queue->dequeue()) {
            $messages[] = $message;
        }
        return $messages;
    }

    private function getMessages($queue, $batchSize)
    {
        $messages = [];
        for ($i = 0; $i < $batchSize; $i++) {
            $message = $queue->dequeue();
            if ($message) {
                $messages[] = $message;
            } else {
                break;
            }
        }
        return $messages;
    }

    private function lockMessages(array $messages)
    {
        $toProcess = [];
        $toAcknowledge = [];
        foreach ($messages as $message) {
            try {
                $this->getMessageController()->lock($message, $this->configuration->getConsumerName());
                $toAcknowledge[] = $message;
            } catch (MessageLockException $exception) {
                $toProcess[] = $message;
            }
        }
        return [$messages, $toAcknowledge];
    }

    private function decodeMessages($messages)
    {
        $decodedMessages = [];
        foreach ($messages as $message) {
            $decodedMessages[] = $this->jsonHelper->jsonDecode($message->getBody());
        }
        return $decodedMessages;
    }

    private function getMessageController()
    {
        $objectManager = \Magento\Framework\App\ObjectManager::getInstance();
        return $objectManager->get(\Magento\Framework\MessageQueue\MessageController::class);
    }
    
    private function moveMessageToDLQ($queue, $message, $retryCount)
    {
        $dlq = $queue->getDeadLetterQueue(); 
        $message->setHeader('dlq_reason', 'Max retry attempts reached.');
        $dlq->send($message);
        $this->logger->info("Order message for order ID: " . $message->getBody() . " has been moved to DLQ after {$retryCount} retries.");
    }
}
