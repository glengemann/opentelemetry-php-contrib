<?php

use OpenTelemetry\API\Common\Instrumentation\Configurator;
use OpenTelemetry\SDK\Trace\Sampler\AlwaysOnSampler;
use OpenTelemetry\SDK\Trace\Sampler\ParentBased;
use OpenTelemetry\SDK\Trace\TracerProviderBuilder;

include __DIR__.'/../vendor/autoload.php';

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
        OpenTelemetry\SemConv\ResourceAttributes::SERVICE_NAME => 'message-producer-php',
        OpenTelemetry\SemConv\ResourceAttributes::PROCESS_RUNTIME_DESCRIPTION => 'RabbitMQ message producer with manual setup for automatic instrumentation.'
    ]);
    $resource = OpenTelemetry\SDK\Resource\ResourceInfo::create($attributes);

    /** @var  OpenTelemetry\SDK\Trace\TracerProvider $tracerProvider */
    $tracerProvider = (new TracerProviderBuilder())
        ->addSpanProcessor($spanProcessor)
        ->addSpanProcessor($jaegerExporter)
        ->setSampler(new ParentBased(new AlwaysOnSampler()))
        ->setResource($resource)
        ->build()
    ;

    OpenTelemetry\SDK\Common\Util\ShutdownHandler::register([$tracerProvider, 'shutdown']);

    return $configurator
        ->withTracerProvider($tracerProvider)
    ;
});
/** Manual setup for automatic instrumentation */

//Establish connection to AMQP
$connection = new AMQPConnection();
$connection->setHost('127.0.0.1');
$connection->setLogin('guest');
$connection->setPassword('guest');
$connection->connect();

//Create and declare channel
$channel = new AMQPChannel($connection);


//AMQP Exchange is the publishing mechanism
$exchange = new AMQPExchange($channel);


try{
	$routing_key = 'hello';

	$queue = new AMQPQueue($channel);
	$queue->setName($routing_key);
	$queue->setFlags(AMQP_NOPARAM);
	$queue->declareQueue();


	$message = 'howdy-do';
	$exchange->publish($message, $routing_key);

	$connection->disconnect();
}catch(Exception $ex){
	print_r($ex);
}
