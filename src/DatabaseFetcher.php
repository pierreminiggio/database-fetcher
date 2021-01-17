<?php

namespace PierreMiniggio\DatabaseFetcher;

use NeutronStars\Database\QueryBuilder;
use PierreMiniggio\DatabaseConnection\DatabaseConnection;
use PierreMiniggio\DatabaseConnection\Exception\ConnectionException as DatabaseConnectionConnectionException;
use PierreMiniggio\DatabaseConnection\Exception\ExecuteException as DatabaseConnectionExecuteException;
use PierreMiniggio\DatabaseConnection\Exception\QueryException as DatabaseConnectionQueryException;
use PierreMiniggio\DatabaseFetcher\Exception\ConnectionException;
use PierreMiniggio\DatabaseFetcher\Exception\ExecuteException;
use PierreMiniggio\DatabaseFetcher\Exception\QueryException;

class DatabaseFetcher
{
    public function __construct(
        private DatabaseConnection $connection,
    )
    {}

    public function createQuery(string|QueryBuilder $table, ?string $alias = null): QueryBuilder
    {
        if ($table instanceof QueryBuilder) {
            return QueryBuilder::create($table, $alias ?? '');
        }

        return new QueryBuilder($alias ? ($table . ' as ' . $alias) : $table);
    }

    /**
     * @throws ConnectionException
     * @throws QueryException
     */
    public function rawQuery(string $query, array $parameters = []): array
    {
        $this->startConnectionOrFail();

        try {
            $res = $this->connection->query($query, $parameters);
        } catch (DatabaseConnectionQueryException $e) {
            $this->connection->stop();
            throw new QueryException($e->getMessage());
        }

        $this->connection->stop();

        return $res;
    }

    /**
     * @throws ConnectionException
     * @throws QueryException
     */
    public function query(QueryBuilder $queryBuilder, array $parameters = []): array
    {
        return $this->rawQuery($queryBuilder->build(), $parameters);
    }

    /**
     * @throws ConnectionException
     * @throws ExecuteException
     */
    public function rawExec(string $query, array $parameters = []): void
    {
        $this->startConnectionOrFail();

        try {
            $this->connection->exec($query, $parameters);
        } catch (DatabaseConnectionExecuteException $e) {
            $this->connection->stop();
            throw new ExecuteException($e->getMessage());
        }

        $this->connection->stop();
    }

    /**
     * @throws ConnectionException
     * @throws ExecuteException
     */
    public function exec(QueryBuilder $queryBuilder, array $parameters = []): void
    {
        $this->rawExec($queryBuilder->build(), $parameters);
    }

    /**
     * @throws ConnectionException
     */
    private function startConnectionOrFail(): void
    {
        try {
            $this->connection->start();
        } catch (DatabaseConnectionConnectionException $e) {
            $this->connection->stop();
            throw new ConnectionException($e->getMessage());
        }
    }
}
