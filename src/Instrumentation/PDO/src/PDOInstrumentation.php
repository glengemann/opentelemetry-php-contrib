<?php

declare(strict_types=1);

namespace OpenTelemetry\Contrib\Instrumentation\PDO;

use OpenTelemetry\API\Common\Instrumentation\CachedInstrumentation;
use OpenTelemetry\API\Common\Instrumentation\Globals;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanInterface;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\Context\Context;
use function OpenTelemetry\Instrumentation\hook;
use OpenTelemetry\SemConv\TraceAttributes;
use Nyholm\Dsn\DsnParser;
use Throwable;

class PDOInstrumentation
{
    public static function register(): void
    {
        $instrumentation = new CachedInstrumentation('io.opentelemetry.contrib.php.pdo');

        hook(
            \PDO::class,
            '__construct',
            pre: static function (\PDO $pdo, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDO::__construct', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT)
                    ->setAttribute(TraceAttributes::DB_CONNECTION_STRING, $params[0] ?? 'unknown')
                    ->setAttribute(TraceAttributes::DB_USER, $params[1] ?? 'unknown')
                    ->setAttribute(TraceAttributes::DB_SYSTEM, self::getDB($params[0]));
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDO $pdo, array $params, mixed $statement, ?Throwable $exception) {
                self::end($exception);            }
        );

        hook(
            \PDO::class,
            'query',
            pre: static function (\PDO $pdo, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDO::query', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT)
                    ->setAttribute(TraceAttributes::DB_STATEMENT, $params[0] ?? 'undefined');
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDO $pdo, array $params, mixed $statement, ?Throwable $exception) {
                self::end($exception);            }
        );

        hook(
            \PDO::class,
            'exec',
            pre: static function (\PDO $pdo, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDO::exec', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT)
                    ->setAttribute(TraceAttributes::DB_STATEMENT, $params[0] ?? 'undefined');
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDO $pdo, array $params, mixed $statement, ?Throwable $exception) {
                self::end($exception);            }
        );

        hook(
            \PDOStatement::class,
            'fetch',
            pre: static function (\PDOStatement $statement, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDOStatement::fetch', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDOStatement $statement, array $params, mixed $retval, ?Throwable $exception) {
                self::end($exception);
            }
        );

        hook(
            \PDOStatement::class,
            'fetchAll',
            pre: static function (\PDOStatement $statement, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDOStatement::fetchAll', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDOStatement $statement, array $params, mixed $retval, ?Throwable $exception) {
                self::end($exception);
            }
        );

        hook(
            \PDOStatement::class,
            'execute',
            pre: static function (\PDOStatement $statement, array $params, string $class, string $function, ?string $filename, ?int $lineno) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'PDOStatement::execute', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\PDOStatement $statement, array $params, mixed $retval, ?Throwable $exception) {
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
        ?int $lineno): SpanBuilderInterface {
        return $instrumentation->tracer()
                    ->spanBuilder($name)
                    ->setAttribute('code.function', $function)
                    ->setAttribute('code.namespace', $class)
                    ->setAttribute('code.filepath', $filename)
                    ->setAttribute('code.lineno', $lineno);
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
    private static function getDB(string $dsn) {
        $scheme = 'undefined';
        try {
            $parsedDsn = DsnParser::parseFunc($dsn);
            $args = $parsedDsn->getArguments();
            if (count($args) > 0) {
                $scheme = $args[0]->getScheme();
            }
        } catch (InvalidDsnException $e) {
            $scheme = 'undefined';
        }
        return $scheme;
    }
}