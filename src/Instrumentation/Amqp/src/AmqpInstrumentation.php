<?php

namespace OpenTelemetry\Contrib\Instrumentation\Amqp;

use OpenTelemetry\API\Common\Instrumentation\CachedInstrumentation;
use OpenTelemetry\API\Common\Instrumentation\Globals;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SDK\Trace\SpanBuilder;
use OpenTelemetry\SDK\Trace\Tracer;
use OpenTelemetry\SemConv\TraceAttributes;
use Throwable;
use function OpenTelemetry\Instrumentation\hook;
use OpenTelemetry\API\Trace\Propagation\TraceContextPropagator;

class AmqpInstrumentation
{
    public static function register(): void
    {
        $instrumentation = new CachedInstrumentation('io.opentelemetry.contrib.php.amqp');

        hook(
            class: \AMQPExchange::class,
            function: 'publish',
            pre: static function (\AMQPExchange $exchange, array $params, string $class, string $function, ?string $filename, ?string $lineno) use ($instrumentation) {
                $builder = self::makeBuilder($instrumentation, 'AMQPExchange::publish', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_PRODUCER);

                $builder
                    ->setAttribute(TraceAttributes::MESSAGING_OPERATION, 'publish')
                    // TODO constant missing
                    ->setAttribute('messaging.destination.name', !empty($exchange->getName()) ? $exchange->getName() : '')
                    // TODO constant missing
                    ->setAttribute('messaging.rabbitmq.destination.routing_key', $params[1] ?? '')
                    ->setAttribute(TraceAttributes::MESSAGING_MESSAGE_ID, $params[3]['message_id'] ?? '')
                ;

                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));

                // TODO tracestate
                $carrier = [];
                TraceContextPropagator::getInstance()->inject($carrier);
                $params[3]['headers'] = $carrier;

                return $params;
            },
            post: static function ($object, ?array $params, mixed $return, ?\Throwable $exception) {
                self::end($exception);
            }
        );

        hook(
            \AMQPQueue::class,
            'consume',
            static function(\AMQPQueue $queue, array $params, string $class, string $function, ?string $filename, ?string $lineno): array {
                $callback = $params[0] ?? null;
                if (!is_callable($callback)) {
                    return [];
                }

                $callback = static function(\AMQPEnvelope $envelope, \AMQPQueue $queue) use ($callback, $class, $function, $filename, $lineno): mixed {
                    static $instrumentation = new CachedInstrumentation('io.opentelemetry.contrib.php.amqp');

                    $context = TraceContextPropagator::getInstance()->extract($envelope->getHeaders());
                    $builder = self::makeBuilder($instrumentation, 'AmqpConsumer::consume', $function, $class, $filename, $lineno)
                        ->setParent($context)
                        ->setSpanKind(SpanKind::KIND_CONSUMER)
                        // TODO messaging.consumer_id or messaging.consumer.id
                        ->setAttribute(TraceAttributes::MESSAGING_CONSUMER_ID, $envelope->getConsumerTag())
                        ->setAttribute('messaging.source.name', $envelope->getExchangeName())
                        ->setAttribute(TraceAttributes::MESSAGING_OPERATION, 'receive')
                        // TODO messaging.message_id or messaging.message.id
                        ->setAttribute(TraceAttributes::MESSAGING_MESSAGE_ID, $envelope->getMessageId())
                        ->setAttribute('messaging.rabbitmq.source.routing_key', $envelope->getRoutingKey())
                    ;

                    $span = $builder->startSpan();
                    $scope = $span->storeInContext($context)->activate();
                    try {
                        return $callback($envelope, $queue);
                    } catch (Throwable $e) {
                        $span->recordException($e, [TraceAttributes::EXCEPTION_ESCAPED => true]);
                        $span->setStatus(StatusCode::STATUS_ERROR, $e->getMessage());

                        throw $e;
                    } finally {
                        $scope->detach();
                        $span->end();
                    }
                };

                return [0 => $callback];
            },
        );

        hook(
            class: \AMQPQueue::class,
            function: 'get',
            pre: static function (\AMQPQueue $queue, array $params, string $class, string $function, ?string $filename, ?string $lineno) use ($instrumentation) {
                $builder = self::makeBuilder($instrumentation, 'AmqpConsumer::get', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CONSUMER)
                    ->setAttribute(TraceAttributes::MESSAGING_OPERATION, 'receive')
                ;

                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function ($object, ?array $params, ?\AMQPEnvelope $return, ?\Throwable $exception) use ($instrumentation) {
                if ($return instanceof \AMQPEnvelope) {
                    $scope = Context::storage()->scope();
                    if (!$scope) {
                        return;
                    }
                    $span = Span::fromContext($scope->context());
                    $span
                        ->setAttribute(TraceAttributes::MESSAGING_CONSUMER_ID, $return->getConsumerTag())
                        ->setAttribute('messaging.source.name', $return->getExchangeName())
                        ->setAttribute(TraceAttributes::MESSAGING_MESSAGE_ID, $return->getMessageId())
                        ->setAttribute('messaging.rabbitmq.source.routing_key', $return->getRoutingKey())
                    ;

                    // TODO
                    $parentContext = TraceContextPropagator::getInstance()->extract($return->getHeaders());
                }

                self::end($exception);
            }
        );

        hook(
            class: \AMQPQueue::class,
            function: 'nack',
            pre: static function (\AMQPQueue $queue, array $params, string $class, string $function, ?string $filename, ?string $lineno) use ($instrumentation) {
                $builder = self::makeBuilder($instrumentation, 'AMQPQueue::nack', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CONSUMER)
                    ->setAttribute(TraceAttributes::MESSAGING_OPERATION, 'process')
                    ->setAttribute(TraceAttributes::MESSAGING_MESSAGE_ID, $params[0] ?? 'unknown')
                ;

                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function ($object, ?array $params, mixed $return, ?\Throwable $exception) {
                self::end($exception);
            }
        );
    }

    private static function makeBuilder(
        CachedInstrumentation $instrumentation,
        string $name,
        string $function,
        string $class,
        ?string $filename,
        ?int $lineno
    ): SpanBuilderInterface {
        /** @psalm-suppress ArgumentTypeCoercion */
        return $instrumentation->tracer()
            ->spanBuilder($name)
            ->setAttribute(TraceAttributes::CODE_FUNCTION, $function)
            ->setAttribute(TraceAttributes::CODE_NAMESPACE, $class)
            ->setAttribute(TraceAttributes::CODE_FILEPATH, $filename)
            ->setAttribute(TraceAttributes::CODE_LINENO, $lineno)
            ->setAttribute(TraceAttributes::MESSAGING_SYSTEM, 'rabbitmq')
            ->setAttribute('net.app.protocol.name', 'amqp')
        ;
    }

    private static function end(?Throwable $exception): void
    {
        $scope = Context::storage()->scope();
        if (!$scope) {
            return;
        }

        $scope->detach();
        $span = Span::fromContext($scope->context());
        if ($exception) {
            $span->recordException($exception, [TraceAttributes::EXCEPTION_ESCAPED => true]);
            $span->setStatus(StatusCode::STATUS_ERROR, $exception->getMessage());
        }
        $span->end();
    }
}
