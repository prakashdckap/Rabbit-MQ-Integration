<?php
namespace Klizer\Srs\Model\Consumer;

use Psr\Log\LoggerInterface;
use Klizer\Srs\Model\OrderProcessor;
use Magento\Framework\MessageQueue\ConsumerInterface;
use Magento\Framework\MessageQueue\QueueRepository;
use Magento\Framework\MessageQueue\MessageEncoder;
use Magento\Framework\Json\Helper\Data as JsonHelper;
use Magento\Framework\MessageQueue\ConsumerConfigurationInterface;
use Magento\Framework\MessageQueue\Consumer\ConfigInterface as ConsumerConfig;
use Magento\Framework\MessageQueue\PublisherInterface;
use Magento\Framework\MessageQueue\Exception\MessageLockException;

class OrderFetchConsumer implements ConsumerInterface
{
    protected $logger;
    protected $orderProcessor;
    protected $queueRepository;
    protected $messageEncoder;
    protected $jsonHelper;
    protected $configuration;
    protected $consumerConfig;
    protected $interval;
    protected $batchSize;
    protected $publisher;

    public function __construct(
        LoggerInterface $logger,
        OrderProcessor $orderProcessor,
        QueueRepository $queueRepository,
        MessageEncoder $messageEncoder,
        JsonHelper $jsonHelper,
        ConsumerConfigurationInterface $configuration,
        ConsumerConfig $consumerConfig,
        PublisherInterface $publisher,
        $interval = 5,
        $batchSize = 10
    ) {
        $this->logger = $logger;
        $this->orderProcessor = $orderProcessor;
        $this->queueRepository = $queueRepository;
        $this->messageEncoder = $messageEncoder;
        $this->jsonHelper = $jsonHelper;
        $this->configuration = $configuration;
        $this->consumerConfig = $consumerConfig;
        $this->publisher = $publisher;
        $this->interval = $interval;
        $this->batchSize = $batchSize;
    }

    public function process($maxNumberOfMessages = null)
    {
        $startTime = microtime(true);
        $this->logger->info("Starting OrderFetchConsumer processing...");

        $totalMessagesProcessed = 0;

        $queueName = $this->configuration->getQueueName();
        $consumerName = $this->configuration->getConsumerName();
        $connectionName = $this->consumerConfig->getConsumer($consumerName)->getConnection();
        $queue = $this->queueRepository->get($connectionName, $queueName);

        if (!isset($maxNumberOfMessages)) {
            $this->runDaemonMode($queue, $totalMessagesProcessed);
        } else {
            $this->run($queue, $maxNumberOfMessages, $totalMessagesProcessed);
        }

        $endTime = microtime(true);
        $executionTime = $endTime - $startTime;

        $this->logger->info("OrderFetchConsumer execution completed in ".number_format($executionTime, 2)." seconds, total messages processed: {$totalMessagesProcessed}.");
    }

    private function runDaemonMode($queue, &$totalMessagesProcessed)
    {
        $this->logger->info("Running in daemon mode with parallel consumers...");
        $maxConsumers = 10;

        for ($consumerId = 0; $consumerId < $maxConsumers; $consumerId++) {
            $pid = pcntl_fork();

            if ($pid == -1) {
                $this->logger->error('Failed to fork a new process for consumer ' . $consumerId);
            } elseif ($pid == 0) {
                $this->logger->info("Consumer {$consumerId} processing started.");
                $this->processConsumer($queue, $totalMessagesProcessed);
                exit(0);
            }
        }

        for ($consumerId = 0; $consumerId < $maxConsumers; $consumerId++) {
            pcntl_wait($status);
        }

        $this->logger->info("Daemon mode completed with parallel consumers.");
    }

    private function processConsumer($queue, &$totalMessagesProcessed)
    {
        $startTime = microtime(true);
        while (true) {
            $messages = $this->batchSize > 0
                ? $this->getMessages($queue, $this->batchSize)
                : $this->getAllMessages($queue);

            if (empty($messages)) {
                $this->logger->info("No more messages for consumer. Sleeping...");
                sleep($this->interval);
                continue;
            }

            $totalMessagesProcessed += count($messages);
            $this->processMessages($queue, $messages);
            $endTime = microtime(true);
            $executionTime = $endTime - $startTime;

            $this->logger->info("OrderFetchConsumer execution completed in ".number_format($executionTime, 2)." seconds, total messages processed: {$totalMessagesProcessed}.");
        }
    }

    private function run($queue, $maxNumberOfMessages, &$totalMessagesProcessed)
    {
        $remainingCount = $maxNumberOfMessages;
        $this->logger->info("Processing {$maxNumberOfMessages} messages...");

        while ($remainingCount > 0) {
            $batchSize = min($remainingCount, $this->batchSize);
            $messages = $this->getMessages($queue, $batchSize);

            $totalMessagesProcessed += count($messages);
            $this->processMessages($queue, $messages);
            $remainingCount -= count($messages);
        }

        $this->logger->info("Processed all {$maxNumberOfMessages} messages.");
    }

    private function processMessages($queue, $messages)
    {
        foreach ($messages as $message) {
            $orderNumber = trim($message->getBody(), '"');
            $orderNumber = stripslashes($orderNumber);
            $orderNumber = json_decode($orderNumber, true);

            if (is_string($orderNumber)) {
                $orderNumber = json_decode($orderNumber, true);
            }

            try {
                if ($orderNumber) {
                    $this->logger->info('Processing order fetch for Order Number: ' . $orderNumber);
                    $orderDetails = $this->orderProcessor->fetchOrderDetails($orderNumber);
                    if ($orderDetails) {
                        $this->publishOrderDetails($orderDetails);
                        $queue->acknowledge($message);
                        $this->logger->info('Successfully acknowledged the message for Order Number: ' . $orderNumber);
                    } else {
                        $this->requeueMessage($queue, $message);
                        $this->logger->error('Failed to fetch order details for Order Number ' . $orderNumber);
                    }
                }
            } catch (\Exception $e) {
                $this->requeueMessage($queue, $message);
                $this->logger->error('Error processing Order Number: ' . $orderNumber);
            }
        }
    }

    private function publishOrderDetails($orderDetails)
    {
        try {
            $this->publisher->publish('order.insert.topic', json_encode($orderDetails));
            $this->logger->info('Order details sent to insert queue.');
        } catch (\Exception $e) {
            $this->logger->error('Failed to publish order details: ');
        }
    }

    private function requeueMessage($queue, $message)
    {
        // Track the retry count in the message body
        $messageData = json_decode($message->getBody(), true);
        $retryCount = isset($messageData['retry_count']) ? $messageData['retry_count'] : 0;
        $maxRetryLimit = 3;

        if ($retryCount >= $maxRetryLimit) {
            // Max retries reached, move to DLQ
            $this->moveToDLQ($queue, $message);
        } else {
            // Increment retry count and requeue message
            $retryCount++;
            $messageData['retry_count'] = $retryCount;
            $message->setBody(json_encode($messageData)); // Update retry count in the message body
            try {
                $this->publisher->publish('order.fetch.topic', json_encode($messageData));
                $this->logger->info('Message requeued for retry, Retry Count: ' . $retryCount);
            } catch (\Exception $e) {
                $this->logger->error('Failed to requeue message for Order Number: ' . $messageData['order_number']);
            }
        }
    }

    private function moveToDLQ($queue, $message)
    {
        // Send the message to the Dead Letter Queue (DLQ)
        $dlqQueue = $queue->getDeadLetterQueue();  // Assuming the queue has a method to get DLQ
        $messageData = json_decode($message->getBody(), true);
        $messageData['error'] = 'Max retries exceeded';
        $message->setBody(json_encode($messageData));

        try {
            $dlqQueue->send($message);  // Move the message to the DLQ
            $this->logger->info('Message for Order Number ' . $messageData['order_number'] . ' moved to DLQ after max retries.');
        } catch (\Exception $e) {
            $this->logger->error('Failed to move message to DLQ: ' . $messageData['order_number']);
        }
    }

    private function getMessages($queue, $count)
    {
        $messages = [];
        for ($i = 0; $i < $count; $i++) {
            $message = $queue->dequeue();
            if ($message) {
                $messages[] = $message;
            } else {
                break;
            }
        }
        return $messages;
    }

    private function getAllMessages($queue)
    {
        $messages = [];
        while ($message = $queue->dequeue()) {
            $messages[] = $message;
        }
        return $messages;
    }
}
