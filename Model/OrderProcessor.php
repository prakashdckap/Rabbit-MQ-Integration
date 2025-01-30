<?php
namespace Klizer\Srs\Model;

use Magento\Framework\App\ResourceConnection;
use Psr\Log\LoggerInterface;
use Magento\Framework\HTTP\Client\Curl;
use Magento\Framework\MessageQueue\PublisherInterface;

class OrderProcessor
{
    protected $logger;
    protected $curlClient;
    protected $resource;
    protected $publisher;

    /**
     * OrderProcessor constructor.
     *
     * @param LoggerInterface $logger
     * @param Curl $curlClient
     * @param ResourceConnection $resource
     */
    public function __construct(
        LoggerInterface $logger,
        Curl $curlClient,
        ResourceConnection $resource,
        PublisherInterface $publisher
    ) {
        $this->logger = $logger;
        $this->curlClient = $curlClient;
        $this->resource = $resource;
        $this->publisher = $publisher;
    }
    /**
     * Fetch order details from an external API using the order number
     *
     * @param string $orderNumber
     * @return array|null
     */
    public function fetchOrderDetails($orderNumber)
    {
        $orderDetails = null;
        $url = 'https://services.heritagelandscapesupplygroup.com/orders/';
        $apiToken = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI3NUI3MUQ4RThBQjY0Q0UwQjBBRkQ0NkJBMUI2QjE0MCIsInVuaXF1ZV9uYW1lIjoiSExTRyIsIkN1c3RvbWVyQ29kZSI6IioiLCJyb2xlIjoiKiIsIm5iZiI6MTczMzMyODAwMCwiZXhwIjoxNzMzNDE0NDAwLCJpYXQiOjE3MzMzMjgwMDB9.A030w82aY4bMhtd_s99PhHblGfNO8kSpltBOpGiCNzM';  // Replace with your actual API token

        try {
            $headers = [
                'Authorization' => 'Bearer ' . $apiToken,
            ];
            $urlWithParams = $url . $orderNumber;

            $this->curlClient->setHeaders($headers);
            $this->curlClient->get($urlWithParams);

            if ($this->curlClient->getStatus() == 200) {
                $responseBody = $this->curlClient->getBody();
                $responseData = json_decode($responseBody, true);

                if (isset($responseData)) {
                    $orderDetails = $responseData;
                }
            } else {
                throw new \Exception('Failed to fetch order details. API response status: ' . $this->curlClient->getStatus());
            }
        } catch (\Exception $e) {
            $this->logger->error("Error fetching details for order {$orderNumber}");
            //$this->logger->error("Error fetching details for order {$orderNumber}: " . $e->getMessage());
        }

        return $orderDetails;
    }

    public function sendOrderToQueue($orderNumber)
    {
        try {
            $this->publisher->publish('order.fetch.topic', $orderNumber);
            $this->logger->info("Order {$orderNumber} sent to the queue.");
        } catch (\Exception $e) {
            $this->logger->error("Error sending order {$orderNumber} to the queue: " . $e->getMessage());
        }
    }
}
