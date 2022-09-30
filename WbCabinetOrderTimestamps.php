<?php
declare(strict_types=1);

namespace App\Lib\WbCabinet;

use App\Lib\ClickHouse\Table\WbCabinetOrdersClickHouseTable;
use App\Lib\Date;
use App\Lib\Time;
use App\Lib\WbCabinet\Entity\WbCabinetSellsItem;

class WbCabinetOrderTimestamps
{
    /** @var int На сколько месяцев искать дату заказа для выкупа */
    private const MAX_MONTHS_ORDER_DATE_SEARCH = 3;

    /** Кол-во элементов для разбиения большого массива */
    private const ARRAY_CHUNK_LENGTH = 500;

    /**
     * Список id заказов и даты заказа
     *
     * @var array<string, string>
     */
    private array $_orderIdTimestamps;

    /**
     * Список id позиции в заказе и даты заказа
     *
     * @var array<string, string>
     */
    private array $_positionIdTimestamps;

    /**
     * Конструктор
     *
     * @param int[] $wbConfigIds
     * @param Date $sellDate
     * @param WbCabinetSellsItem[] $items
     */
    public function __construct(array $wbConfigIds, Date $sellDate, array $items)
    {
        $orderIds = [];
        $orderPositionIds = [];
        foreach ($items as $item) {
            if (!empty($item->orderId)) {
                $orderIds[] = $item->orderId;
            }
            if (!empty($item->orderPositionId)) {
                $orderPositionIds[] = $item->orderPositionId;
            }
        }

        $this->_orderIdTimestamps = $this->_getOrderTimestamps($wbConfigIds, $sellDate, 'orderId', array_unique($orderIds));
        $this->_positionIdTimestamps = $this->_getOrderTimestamps($wbConfigIds, $sellDate, 'orderPositionId', array_unique($orderPositionIds));
    }

    /**
     * Получить макс дату-время по заказу
     *
     * @param string $orderId
     * @param string $orderPositionId
     * @return Time|null
     */
    public function getMaxTime(string $orderId, string $orderPositionId): ?Time
    {
        $orderDate = $this->_orderIdTimestamps[$this->_getStringKey($orderId)] ?? null;
        $positionDate = $this->_positionIdTimestamps[$this->_getStringKey($orderPositionId)] ?? null;

        $resultDate = max($orderDate, $positionDate);

        return $resultDate ? Time::parse($resultDate) : null;
    }

    /**
     * Ищем дату заказа
     *
     * @param int[] $wbConfigIds
     * @param Date $sellDate
     * @param string $filedName
     * @param string[] $ids
     * @return array<string, string>
     */
    private function _getOrderTimestamps(array $wbConfigIds, Date $sellDate, string $filedName, array $ids): array
    {
        if (empty($ids)) {
            return [];
        }

        $result = [];
        $idsChunks = array_chunk($ids, self::ARRAY_CHUNK_LENGTH);
        $table = WbCabinetOrdersClickHouseTable::getInstance();

        foreach ($idsChunks as $idsChunk) {
            $rows = $table->select(
                '
                SELECT {filedName}, orderDate
                FROM {wbCabinetOrders}
                WHERE wbConfigId IN (:wbConfigIds)
                    AND orderDate BETWEEN :minOrderDate AND :maxOrderDate
                    AND {filedName} IN (:ids)
                ORDER BY {filedName}, orderDate
                ',
                [
                    'wbCabinetOrders' => $table->getTableName(),
                    'wbConfigIds' => $wbConfigIds,
                    'minOrderDate' => Time::parse($sellDate)->subMonths(self::MAX_MONTHS_ORDER_DATE_SEARCH)->startOfDay()->toDateTimeString(),
                    'maxOrderDate' => Time::parse($sellDate)->endOfDay()->toDateTimeString(),
                    'ids' => $idsChunk,
                    'filedName' => $filedName
                ]
            )->rows();

            foreach ($rows as $row) {
                $stringKey = $this->_getStringKey($row[$filedName]);
                if (empty($result[$stringKey]) || $row['orderDate'] > $result[$stringKey]) {
                    $result[$stringKey] = $row['orderDate'];
                }
            }
        }

        return $result;
    }

    /**
     * Добавить к Id префикс, чтобы избежать автоконвертации в число в ключе массива
     *
     * @param string $id
     * @return string
     */
    private function _getStringKey(string $id): string
    {
        return '_' . $id;
    }
}
