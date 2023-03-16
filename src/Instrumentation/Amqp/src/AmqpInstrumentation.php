<?php

namespace OpenTelemetry\Contrib\Instrumentation\Amqp;

use OpenTelemetry\API\Common\Instrumentation\CachedInstrumentation;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\Context\Context;
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

                $builder->setAttribute('messaging.operation', 'publish');
                // Only for spans that represent an operation on a single message.
                //$builder->setAttribute('messaging.message.id', );

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
            class: \AMQPQueue::class,
            function: 'consume',
            pre: static function (\AMQPQueue $queue, array $params, string $class, string $function, ?string $filename, ?string $lineno) use ($instrumentation) {
                $builder = self::makeBuilder($instrumentation, 'AMQPQueue::consume', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CONSUMER);
                $builder->setAttribute('consumer.tag', $params[2] ?? 'unknown');
                $builder->setAttribute(TraceAttributes::MESSAGING_CONSUMER_ID, $params[2] ?? 'unknown');
                $builder->setAttribute('messaging.consume.flags', $params[1] ?? 'unknown');

                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            //post: static function ($object, ?array $params, mixed $return, \Throwable|\UnwindExit|null $exception) {
            post: static function ($object, ?array $params, mixed $return, $exception) {
                if ($exception instanceof \Exception) {
                    self::end($exception);
                } else {
                    self::end(null);
                }
            }
        );

        hook(
            class: \AMQPQueue::class,
            function: 'nack',
            pre: static function (\AMQPQueue $queue, array $params, string $class, string $function, ?string $filename, ?string $lineno) use ($instrumentation) {
                $builder = self::makeBuilder($instrumentation, 'AMQPQueue::nack', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CONSUMER);

                $builder->setAttribute('messaging.operation', 'process');
                $builder->setAttribute(TraceAttributes::MESSAGING_MESSAGE_ID, $params[0] ?? 'unknown');

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
            ->setAttribute('code.function', $function)
            ->setAttribute('code.namespace', $class)
            ->setAttribute('code.filepath', $filename)
            ->setAttribute('code.lineno', $lineno)
            ->setAttribute('messaging.system', 'rabbitmq')
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
