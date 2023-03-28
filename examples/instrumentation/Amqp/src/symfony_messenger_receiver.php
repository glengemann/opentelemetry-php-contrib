<?php

use OpenTelemetry\API\Common\Instrumentation\Configurator;
use OpenTelemetry\SDK\Trace\Sampler\AlwaysOnSampler;
use OpenTelemetry\SDK\Trace\Sampler\ParentBased;
use OpenTelemetry\SDK\Trace\TracerProviderBuilder;
use Symfony\Component\Messenger\Bridge\Amqp\Transport\AmqpReceiver;
use Symfony\Component\Messenger\Bridge\Amqp\Transport\Connection;
use Symfony\Component\Messenger\Envelope;

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
        OpenTelemetry\SemConv\ResourceAttributes::SERVICE_NAME => 'message-consumer-symfony-messenger',
        OpenTelemetry\SemConv\ResourceAttributes::PROCESS_RUNTIME_DESCRIPTION => 'RabbitMQ message consumer using symfony/amqp-messenger with manual setup for automatic instrumentation.'
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

class SmsNotification
{
    public function __construct(private string $content)
    {
    }

    public function getContent(): string
    {
        return $this->content;
    }
}

$connectionOptions = [];
$exchangeOptions = ['name' => 'otl-ex'];
$queuesOptions = ['otl-q' => ['']];
$connection = new Connection($connectionOptions, $exchangeOptions, $queuesOptions);

$receiver = new AmqpReceiver($connection);
$messages = iterator_to_array($receiver->get());

$receiver->reject($messages[0]);


