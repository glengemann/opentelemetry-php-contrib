<?php

use OpenTelemetry\API\Common\Instrumentation\Configurator;
use OpenTelemetry\SDK\Trace\Sampler\AlwaysOnSampler;
use OpenTelemetry\SDK\Trace\Sampler\ParentBased;
use OpenTelemetry\SDK\Trace\TracerProviderBuilder;

include __DIR__ . '/../vendor/autoload.php';

/** Manual setup for automatic instrumentation */
OpenTelemetry\API\Common\Instrumentation\Globals::registerInitializer(function (Configurator $configurator) {
    $grpcTransport = new OpenTelemetry\Contrib\Grpc\GrpcTransportFactory();
    $endpoint = sprintf(
        'http://localhost:4317%s',
        OpenTelemetry\Contrib\Otlp\OtlpUtil::method(OpenTelemetry\API\Common\Signal\Signals::TRACE)
    );
    $transport = $grpcTransport->create($endpoint);
    $jaegerExporter = new \OpenTelemetry\Contrib\Otlp\SpanExporter($transport);
    $jaegerExporter = new \OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor($jaegerExporter);

    $exporter = new \OpenTelemetry\SDK\Trace\SpanExporter\ConsoleSpanExporterFactory();
    $spanProcessor = new \OpenTelemetry\SDK\Trace\SpanProcessor\SimpleSpanProcessor($exporter->create());

    $attributes = OpenTelemetry\SDK\Common\Attribute\Attributes::create([
        OpenTelemetry\SemConv\ResourceAttributes::SERVICE_NAME => 'message-consumer-consume-php',
        OpenTelemetry\SemConv\ResourceAttributes::PROCESS_RUNTIME_DESCRIPTION => 'RabbitMQ message consumer using Basic.consume with manual setup for automatic instrumentation.'
    ]);
    $resource = OpenTelemetry\SDK\Resource\ResourceInfo::create($attributes);

    /** @var  OpenTelemetry\SDK\Trace\TracerProvider $tracerProvider */
    $tracerProvider = (new TracerProviderBuilder())
        ->addSpanProcessor($spanProcessor)
        ->addSpanProcessor($jaegerExporter)
        ->setSampler(new ParentBased(new AlwaysOnSampler()))
        ->setResource($resource)
        ->build();

    OpenTelemetry\SDK\Common\Util\ShutdownHandler::register([$tracerProvider, 'shutdown']);

    return $configurator
        ->withTracerProvider($tracerProvider);
});
/** Manual setup for automatic instrumentation */

//Establish connection AMQP
$connection = new AMQPConnection();
$connection->setHost('127.0.0.1');
$connection->setLogin('guest');
$connection->setPassword('guest');
$connection->connect();

//Create and declare channel
$channel = new AMQPChannel($connection);

//AMQP Exchange is the publishing mechanism
$exchange = new AMQPExchange($channel);
$exchange->setName('otl-ex');

$callback_func = function (AMQPEnvelope $message, AMQPQueue $q) {
    $body = json_decode($message->getBody(), true);
    echo ' [x] Received order number: ', $body['order_id'], PHP_EOL;

    $q->nack($message->getDeliveryTag());

    sleep(2);
};

try {
    $queue = new AMQPQueue($channel);
    $queue->setName('otl-q');
    $queue->setFlags(AMQP_DURABLE);
    $queue->bind($exchange->getName(), 'new.order');
    $queue->declareQueue();

    echo ' [*] Waiting for messages. To exit press CTRL+C ', PHP_EOL;
    $queue->consume($callback_func);
} catch (AMQPQueueException $ex) {
    print_r($ex);
} catch (Exception $ex) {
    print_r($ex);
}

echo 'Close connection...', PHP_EOL;
$queue->cancel();
$connection->disconnect();

