<?php
namespace Klizer\Srs\Controller\Order;

use Magento\Framework\App\Action\Action;
use Magento\Framework\App\Action\Context;
use Magento\Framework\HTTP\Client\Curl;
use Klizer\Srs\Model\OrderProcessor;
use Psr\Log\LoggerInterface;
use GuzzleHttp\Client;
use GuzzleHttp\Promise;
use GuzzleHttp\Pool;

class OrderController extends Action
{
    protected $curlClient;
    protected $orderProcessor;
    protected $logger;
    protected $guzzleClient;

    public function __construct(Context $context, Curl $curlClient, OrderProcessor $orderProcessor, LoggerInterface $logger)
    {
        parent::__construct($context);
        $this->curlClient = $curlClient;
        $this->orderProcessor = $orderProcessor;
        $this->logger = $logger;
        $this->guzzleClient = new Client(); // Instantiate Guzzle client
    }

    public function execute()
    {
        $timestamp = $this->getRequest()->getParam('timestamp');

        if (!$timestamp) {
            $this->getResponse()->setHttpResponseCode(400);
            $this->getResponse()->setBody('Timestamp is required.');
            return;
        }

        try {
            // Track the start time
            $startTime = microtime(true);

            // Step 1: Fetch order numbers from external API
            $orderNumbers = $this->fetchOrderNumbers($timestamp);
            $totalOrders = count($orderNumbers);  // Total number of orders

            if ($totalOrders == 0) {
                $this->getResponse()->setHttpResponseCode(404);
                $this->getResponse()->setBody('No orders found.');
                return;
            }

            $this->logger->info("Started Order Details Fetching Process for {$totalOrders} orders..");

            // Step 2: Prepare Guzzle requests with Pool for concurrency
            $requests = [];
            foreach (array_slice($orderNumbers, 0,500) as $orderNumber) {
                $requests[] = function() use ($orderNumber) {
                };
            }

            // Step 3: Create a pool to manage concurrency
            $pool = new Pool($this->guzzleClient, $requests, [
                'concurrency' => 10,  // Max number of concurrent requests
                'fulfilled' => function($response, $index) use ($orderNumbers) {
                    $orderNumber = $orderNumbers[$index];
                    if ($orderNumber) {
                        $this->logger->info("Fetched details for order: {$orderNumber}");
                        // Send the order Number to RabbitMQ
                        $this->orderProcessor->sendOrderToQueue($orderNumber);
                    } else {
                        $this->logger->error("No details found for order: {$orderNumber}");
                    }
                },
                'rejected' => function($reason, $index) use ($orderNumbers) {
                    $orderNumber = $orderNumbers[$index];
                    $this->logger->error("Failed to fetch details for order {$orderNumber}: " . $reason);
                },
            ]);

            // Start the pool of requests
            $pool->promise()->wait();

            // Track the end time
            $endTime = microtime(true);
            $totalTime = round($endTime - $startTime, 2); // Total processing time in seconds

            // Log the total processing time and total number of orders processed
            $this->logger->info("Process completed. Total orders processed: {$totalOrders}. Total processing time: {$totalTime} seconds.");

            $this->getResponse()->setBody('Orders are being processed.');
        } catch (\Exception $e) {
            $this->logger->error('Error processing orders: ' . $e->getMessage());
            $this->getResponse()->setHttpResponseCode(500);
            $this->getResponse()->setBody('Error processing orders: ' . $e->getMessage());
        }
    }

    /**
     * Fetch order numbers from external API
     */
    private function fetchOrderNumbers($timestamp)
    {
        $orderNumbers = [];
        $url = 'https://services.heritagelandscapesupplygroup.com/orders/ChangeList';
        $apiToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI3NUI3MUQ4RThBQjY0Q0UwQjBBRkQ0NkJBMUI2QjE0MCIsInVuaXF1ZV9uYW1lIjoiSExTRyIsIkN1c3RvbWVyQ29kZSI6IioiLCJyb2xlIjoiKiIsIm5iZiI6MTczMzMyODAwMCwiZXhwIjoxNzMzNDE0NDAwLCJpYXQiOjE3MzMzMjgwMDB9.A030w82aY4bMhtd_s99PhHblGfNO8kSpltBOpGiCNzM'; // Replace with your actual token

        try {
            $headers = [
                'Authorization' => 'Bearer ' . $apiToken,
            ];
            $urlWithParams = $url . '?timestamp=' . urlencode($timestamp);

            $this->curlClient->setHeaders($headers);
            $this->curlClient->get($urlWithParams);

            if ($this->curlClient->getStatus() == 200) {
                $responseBody = $this->curlClient->getBody();
                $responseData = json_decode($responseBody, true);

                if (isset($responseData['orderList']) && is_array($responseData['orderList'])) {
                    foreach ($responseData['orderList'] as $order) {
                        if (isset($order['orderNumber'])) {
                            $orderNumbers[] = $order['orderNumber'];
                        }
                    }
                }
            } else {
                throw new \Exception('Failed to fetch order numbers.');
            }
        } catch (\Exception $e) {
            $this->logger->error('Error fetching order numbers: ' . $e->getMessage());
        }

        return $orderNumbers;
    }
}

