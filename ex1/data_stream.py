#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):

    __static_last_stream_id = 0

    stream_id: str
    stream_type: str = "Abstract Data"
    _stats: Dict[str, Union[str, int, float]]
    _keys: List[str]

    def __init__(
        self,
        stream_id: str = "DATA",
        keys: Optional[List[str]] = None
    ) -> None:
        self.__class__.__static_last_stream_id += 1
        self.stream_id = (
            f"{stream_id}_" +
            f"{self.__class__.__static_last_stream_id:03}"
        )
        self._stats = {}
        if keys is None:
            keys = []
        self._keys = keys

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    @abstractmethod
    def _data_is_high_priority(
        self,
        data: Any
    ) -> bool:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        if criteria is None:
            return [
                data for data in data_batch
                if self.can_handle(data)
            ]
        if criteria == "high_priority":
            return [
                data for data in data_batch
                if (
                    self.can_handle(data)
                    and self._data_is_high_priority(data)
                )
            ]
        raise ValueError(f"Unhandled filtering criteria {criteria}")

    def get_stats(
        self
    ) -> Dict[str, Union[str, int, float]]:
        return self._stats

    def reset_stats(self) -> None:
        self._stats.clear()

    @abstractmethod
    def can_handle(self, data: Any) -> bool:
        pass

    def _update_stats(self, key: str, value: str | float) -> None:
        old_value = self._stats.get(key) or 0
        if (
            (
                "total" in key
                or "count" in key
            )
            and (
                isinstance(old_value, int)
                or isinstance(old_value, float)
            ) and (
                isinstance(value, int)
                or isinstance(value, float)
            )
        ):
            self._stats[key] = old_value + value
        else:
            self._stats[key] = value


class SensorStream(DataStream):

    stream_type: str = "Environmental Data"

    def __init__(self) -> None:
        super().__init__("SENSOR", keys=[
            "temp",
            "humidity",
            "pressure"
        ])

    def process_batch(self, data_batch: List[str]) -> str:
        splitted_data: List[str]
        processed_batch = "["
        batch_len = len(data_batch)
        temp_sum = 0.0
        temp_readings_count = 0
        i = 0

        for data in data_batch:
            if not self.can_handle(data):
                raise ValueError(f"SensorStream cannot handle {data}")
            splitted_data = data.split(":")
            processed_batch += f"{splitted_data[0]}:{splitted_data[1]}"
            i += 1
            if i != batch_len:
                processed_batch += ", "
            if splitted_data[0] == "temp":
                temp_sum += float(splitted_data[1])
                temp_readings_count += 1
        processed_batch += "]"
        if temp_readings_count > 0:
            self._update_stats("temp_avg", temp_sum / temp_readings_count)
        self._update_stats("total_readings", batch_len)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        splitted_data: List[str]

        if not isinstance(data, str):
            return False
        splitted_data = data.split(":")
        if len(splitted_data) != 2:
            return False
        if splitted_data[0] not in self._keys:
            return False
        try:
            float(splitted_data[1])
        except ValueError:
            return False
        return True

    def _data_is_high_priority(
        self,
        data: str
    ) -> bool:
        splitted_data = data.split(":")
        key = splitted_data[0]
        reading = float(splitted_data[1])
        if key == "temp" and reading > 40:
            return True
        elif key == "humidity" and reading > 90:
            return True
        elif key == "pressure" and reading > 1040:
            return True
        return False


class TransactionStream(DataStream):

    stream_type: str = "Financial Data"

    def __init__(self) -> None:
        super().__init__("TRANSACTION", [
            "buy",
            "sell"
        ])

    def process_batch(self, data_batch: List[Any]) -> str:
        splitted_data: List[str]
        processed_batch = "["
        batch_len = len(data_batch)
        net_flow = 0
        i = 0

        for data in data_batch:
            if not self.can_handle(data):
                raise ValueError(f"TransactionStream cannot handle {data}")
            splitted_data = data.split(":")
            processed_batch += f"{splitted_data[0]}:{splitted_data[1]}"
            i += 1
            if i != batch_len:
                processed_batch += ", "
            if splitted_data[0] == "buy":
                net_flow -= int(splitted_data[1])
            else:
                net_flow += int(splitted_data[1])
        processed_batch += "]"
        if net_flow < 0:
            net_flow_str = f"{net_flow}"
        else:
            net_flow_str = f"+{net_flow}"

        self._update_stats("total_operations", batch_len)
        self._update_stats("net_flow", net_flow_str)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        splitted_data: List[str]

        if not isinstance(data, str):
            return False
        splitted_data = data.split(":")
        if len(splitted_data) != 2:
            return False
        if splitted_data[0] not in self._keys:
            return False
        try:
            int(splitted_data[1])
        except ValueError:
            return False
        return True

    def _data_is_high_priority(
        self,
        data: str
    ) -> bool:
        amount = int(data.split(":")[1])
        return amount >= 150


class EventStream(DataStream):

    def __init__(self) -> None:
        super().__init__("EVENT", [
            "login",
            "error",
            "logout"
        ])

    def process_batch(self, data_batch: List[Any]) -> str:
        processed_batch = "["
        batch_len = len(data_batch)
        error_count = 0
        i = 0

        for data in data_batch:
            if not self.can_handle(data):
                raise ValueError(f"EventStream cannot handle {data}")
            processed_batch += f"{data}"
            i += 1
            if i != batch_len:
                processed_batch += ", "
            if data == "error":
                error_count += 1
        processed_batch += "]"
        self._update_stats("total_events", batch_len)
        self._update_stats("error_count", error_count)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        if not isinstance(data, str):
            return False
        if data not in self._keys:
            return False
        return True

    def _data_is_high_priority(
        self,
        data: str
    ) -> bool:
        return data == "error"


class StreamProcessor:
    __batches: Dict[str, List[Any]]

    def __init__(self) -> None:
        self.__batches = {}

    def add_batch(self, stream_ids: List[str], batch: List[Any]) -> None:
        for stream_id in stream_ids:
            if self.__batches.get(stream_id) is None:
                self.__batches[stream_id] = []
            self.__batches[stream_id].extend(batch)

    def process_stream(
        self,
        stream: DataStream,
        high_priority: bool = False
    ) -> Dict[str, Union[str, int, float]]:
        criteria: Optional[str] = None

        stream.reset_stats()
        batch = self.__batches.get(stream.stream_id)
        if batch is None or not len(batch):
            raise ValueError(
                "Error: No batch associated with" +
                f" DataStream with id {stream.stream_id}"
            )
        if high_priority:
            criteria = "high_priority"
        batch = stream.filter_data(batch, criteria)
        if batch is None or not len(batch):
            return stream.get_stats()
        stream.process_batch(batch)
        return stream.get_stats()

    def process_mixed_stream(
        self,
        streams: List[DataStream],
        high_priority: bool = False
    ) -> Dict[str, Dict[str, Union[str, int, float]]]:

        batch: List[Any]
        filtered_batch: List[Any]
        stats: Dict[str, Dict[str, Union[str, int, float]]] = {}
        criteria: Optional[str] = None

        if high_priority:
            criteria = "high_priority"
        for stream in streams:
            stream.reset_stats()
            batch = self.__batches.get(stream.stream_id) or []
            filtered_batch = stream.filter_data(batch, criteria)
            stream.process_batch(filtered_batch)
            stats[stream.stream_id] = stream.get_stats()
        return stats


def test_sensor_stream_can_handle() -> None:
    print(" Testing can_handle method of SensorStream")
    stream = SensorStream()
    print(
        "=> SensorStream should handle",
        "[valid token]:[valid number]"
    )
    assert stream.can_handle("temp:42"), \
        "SensorStream should handle 'temp:42'"
    assert stream.can_handle("humidity:42"), \
        "SensorStream should handle 'humidity:42'"
    assert stream.can_handle("pressure:42"), \
        "SensorStream should handle 'pressure:42'"
    print(" OK")
    print(
        "=> SensorStream should handle",
        "[valid token]:[valid float number]"
    )
    assert stream.can_handle("temp:42.42"), \
        "SensorStream should handle 'temp:42.42'"
    assert stream.can_handle("humidity:42.42"), \
        "SensorStream should handle 'humidity:42.42'"
    assert stream.can_handle("pressure:42.42"), \
        "SensorStream should handle 'pressure:42.42'"
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[valid token]:[invalid number]"
    )
    assert not stream.can_handle("temp:abc"), \
        "SensorStream should not handle 'temp:abc'"
    assert not stream.can_handle("humidity:abc"), \
        "SensorStream should not handle 'humidity:abc'"
    assert not stream.can_handle("pressure:abc"), \
        "SensorStream should not handle 'pressure:abc'"
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[invalid token]:[valid number]"
    )
    assert not stream.can_handle("abc:42"), \
        "SensorStream should not handle 'abc:42'"
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[invalid token]:[invalid number]"
    )
    assert not stream.can_handle("abc:abc"), \
        "SensorStream should not handle 'abc:abc'"
    print(" OK")


def test_sensor_stream_process_batch() -> None:
    print(" Testing process_batch method of SensorStream")
    stream = SensorStream()
    print("=> process_batch should return a formatted string")
    result = stream.process_batch([
        "temp:22.5",
        "humidity:65",
        "pressure:1013"
    ])
    assert result == "[temp:22.5, humidity:65, pressure:1013]", \
        f"Expected '[temp:22.5, humidity:65, pressure:1013]', got '{result}'"
    print(" OK")
    print("=> process_batch should update total_readings stat")
    stats = stream.get_stats()
    assert stats["total_readings"] == 3, \
        f"Expected total_readings=3, got {stats['total_readings']}"
    print(" OK")
    print("=> process_batch should compute temp_avg stat")
    assert stats["temp_avg"] == 22.5, \
        f"Expected temp_avg=22.5, got {stats['temp_avg']}"
    print(" OK")
    print("=> process_batch should accumulate stats across multiple calls")
    stream.process_batch(["temp:24.5", "humidity:70"])
    stats = stream.get_stats()
    assert stats["total_readings"] == 5, \
        f"Expected total_readings=5, got {stats['total_readings']}"
    assert stats["temp_avg"] == 24.5, \
        f"Expected temp_avg=47.0, got {stats['temp_avg']}"
    print(" OK")
    print("=> process_batch should raise ValueError on invalid data")
    try:
        stream.process_batch(["temp:abc"])
        assert False, "Should have raised ValueError on invalid number"
    except ValueError:
        pass
    print(" OK")
    print("=> process_batch should raise ValueError on unknown key")
    try:
        stream.process_batch(["wind:42"])
        assert False, "Should have raised ValueError on unknown key 'wind'"
    except ValueError:
        pass
    print(" OK")


def test_sensor_stream_filter_data() -> None:
    print(" Testing filter_data method of SensorStream")
    stream = SensorStream()
    print("=> filter_data without criteria should remove invalid data")
    result = stream.filter_data([
        "temp:22.5", "invalid", "humidity:65", None, "pressure:1013"
    ])
    assert result == ["temp:22.5", "humidity:65", "pressure:1013"], \
        f"Expected valid entries only, got {result}"
    print(" OK")
    print(
        "=> filter_data with high_priority",
        "should keep only critical readings"
    )
    result = stream.filter_data([
        "temp:22.5",
        "temp:45.0",
        "humidity:65",
        "humidity:95.0",
        "pressure:1013"
    ], criteria="high_priority")
    assert result == ["temp:45.0", "humidity:95.0"], \
        f"Expected only high priority readings, got {result}"
    print(" OK")
    print("=> filter_data with unknown criteria should raise ValueError")
    try:
        stream.filter_data(["temp:22.5"], criteria="unknown")
        assert False, "Should have raised ValueError on unknown criteria"
    except ValueError:
        pass
    print(" OK")


def test_sensor_stream() -> None:
    print("Testing SensorStream")
    test_sensor_stream_can_handle()
    test_sensor_stream_process_batch()
    test_sensor_stream_filter_data()
    print("SensorStream OK")


def test_transaction_stream_can_handle() -> None:
    print(" Testing can_handle method of TransactionStream")
    stream = TransactionStream()
    print("=> TransactionStream should handle [valid token]:[valid integer]")
    assert stream.can_handle("buy:100"), \
        "TransactionStream should handle 'buy:100'"
    assert stream.can_handle("sell:150"), \
        "TransactionStream should handle 'sell:150'"
    print(" OK")
    print("=> TransactionStream should not handle [valid token]:[float]")
    assert not stream.can_handle("buy:42.5"), \
        "TransactionStream should not handle 'buy:42.5'"
    print(" OK")
    print(
        "=> TransactionStream should not handle",
        "[valid token]:[invalid number]"
    )
    assert not stream.can_handle("buy:abc"), \
        "TransactionStream should not handle 'buy:abc'"
    assert not stream.can_handle("sell:abc"), \
        "TransactionStream should not handle 'sell:abc'"
    print(" OK")
    print(
        "=> TransactionStream should not handle",
        "[invalid token]:[valid number]"
    )
    assert not stream.can_handle("temp:42"), \
        "TransactionStream should not handle 'temp:42'"
    assert not stream.can_handle("abc:42"), \
        "TransactionStream should not handle 'abc:42'"
    print(" OK")


def test_transaction_stream_process_batch() -> None:
    print(" Testing process_batch method of TransactionStream")
    stream = TransactionStream()
    print("=> process_batch should return a formatted string")
    result = stream.process_batch(["buy:100", "sell:150", "buy:75"])
    assert result == "[buy:100, sell:150, buy:75]", \
        f"Expected '[buy:100, sell:150, buy:75]', got '{result}'"
    print(" OK")
    print("=> process_batch should update total_operations stat")
    stats = stream.get_stats()
    assert stats["total_operations"] == 3, \
        f"Expected total_operations=3, got {stats['total_operations']}"
    print(" OK")
    print("=> process_batch should compute net_flow stat")
    assert stats["net_flow"] == "-25", \
        f"Expected net_flow=\"-25\", got \"{stats['net_flow']}\""
    print(" OK")
    print("=> process_batch should accumulate stats across multiple calls")
    stream.process_batch(["sell:200", "buy:50"])
    stats = stream.get_stats()
    assert stats["total_operations"] == 5, \
        f"Expected total_operations=5, got {stats['total_operations']}"
    assert stats["net_flow"] == "+150", \
        f"Expected net_flow=\"+125\", got \"{stats['net_flow']}\""
    print(" OK")
    print("=> process_batch should raise ValueError on invalid data")
    try:
        stream.process_batch(["buy:abc"])
        assert False, "Should have raised ValueError on invalid number"
    except ValueError:
        pass
    print(" OK")
    print("=> process_batch should raise ValueError on unknown key")
    try:
        stream.process_batch(["transfer:100"])
        assert False, "Should have raised ValueError on unknown key 'transfer'"
    except ValueError:
        pass
    print(" OK")


def test_transaction_stream_filter_data() -> None:
    print(" Testing filter_data method of TransactionStream")
    stream = TransactionStream()
    print("=> filter_data without criteria should remove invalid data")
    result = stream.filter_data([
        "buy:100", "invalid", "sell:150", None, "temp:42"
    ])
    assert result == ["buy:100", "sell:150"], \
        f"Expected valid entries only, got {result}"
    print(" OK")
    print(
        "=> filter_data with high_priority",
        "should keep only large transactions"
    )
    result = stream.filter_data([
        "buy:100", "sell:150", "buy:75", "sell:200"
    ], criteria="high_priority")
    assert result == ["sell:150", "sell:200"], \
        f"Expected only high priority transactions, got {result}"
    print(" OK")
    print("=> filter_data with unknown criteria should raise ValueError")
    try:
        stream.filter_data(["buy:100"], criteria="unknown")
        assert False, "Should have raised ValueError on unknown criteria"
    except ValueError:
        pass
    print(" OK")


def test_transaction_stream() -> None:
    print("Testing TransactionStream")
    test_transaction_stream_can_handle()
    test_transaction_stream_process_batch()
    test_transaction_stream_filter_data()
    print("TransactionStream OK")


def test_event_stream_can_handle() -> None:
    print(" Testing can_handle method of EventStream")
    stream = EventStream()
    print("=> EventStream should handle valid event types")
    assert stream.can_handle("login"), \
        "EventStream should handle 'login'"
    assert stream.can_handle("logout"), \
        "EventStream should handle 'logout'"
    assert stream.can_handle("error"), \
        "EventStream should handle 'error'"
    print(" OK")
    print("=> EventStream should not handle unknown event types")
    assert not stream.can_handle("transfer"), \
        "EventStream should not handle 'transfer'"
    assert not stream.can_handle("temp:42"), \
        "EventStream should not handle 'temp:42'"
    assert not stream.can_handle(""), \
        "EventStream should not handle empty string"
    print(" OK")
    print("=> EventStream should not handle non-string data")
    assert not stream.can_handle(42), \
        "EventStream should not handle integer 42"
    assert not stream.can_handle(None), \
        "EventStream should not handle None"
    print(" OK")


def test_event_stream_process_batch() -> None:
    print(" Testing process_batch method of EventStream")
    stream = EventStream()
    print("=> process_batch should return a formatted string")
    result = stream.process_batch(["login", "error", "logout"])
    assert result == "[login, error, logout]", \
        f"Expected '[login, error, logout]', got '{result}'"
    print(" OK")
    print("=> process_batch should update total_events stat")
    stats = stream.get_stats()
    assert stats["total_events"] == 3, \
        f"Expected total_events=3, got {stats['total_events']}"
    print(" OK")
    print("=> process_batch should count errors")
    assert stats["error_count"] == 1, \
        f"Expected error_count=1, got {stats['error_count']}"
    print(" OK")
    print("=> process_batch should accumulate stats across multiple calls")
    stream.process_batch(["error", "error", "login"])
    stats = stream.get_stats()
    assert stats["total_events"] == 6, \
        f"Expected total_events=6, got {stats['total_events']}"
    assert stats["error_count"] == 3, \
        f"Expected error_count=3, got {stats['error_count']}"
    print(" OK")
    print("=> process_batch should raise ValueError on unknown event")
    try:
        stream.process_batch(["transfer"])
        assert False, \
            "Should have raised ValueError on unknown event 'transfer'"
    except ValueError:
        pass
    print(" OK")
    print("=> process_batch should raise ValueError on non-string data")
    try:
        stream.process_batch([42])
        assert False, "Should have raised ValueError on non-string data"
    except ValueError:
        pass
    print(" OK")


def test_event_stream_filter_data() -> None:
    print(" Testing filter_data method of EventStream")
    stream = EventStream()
    print("=> filter_data without criteria should remove invalid data")
    result = stream.filter_data([
        "login", "unknown_event", "error", None, "logout"
    ])
    assert result == ["login", "error", "logout"], \
        f"Expected valid entries only, got {result}"
    print(" OK")
    print("=> filter_data with high_priority should keep only errors")
    result = stream.filter_data([
        "login", "error", "logout", "error"
    ], criteria="high_priority")
    assert result == ["error", "error"], \
        f"Expected only error events, got {result}"
    print(" OK")
    print("=> filter_data with unknown criteria should raise ValueError")
    try:
        stream.filter_data(["login"], criteria="unknown")
        assert False, "Should have raised ValueError on unknown criteria"
    except ValueError:
        pass
    print(" OK")


def test_event_stream() -> None:
    print("Testing EventStream")
    test_event_stream_can_handle()
    test_event_stream_process_batch()
    test_event_stream_filter_data()
    print("EventStream OK")


def test_stream_processor_add_batch() -> None:
    print(" Testing add_batch method of StreamProcessor")
    processor = StreamProcessor()
    sensor = SensorStream()
    print("=> add_batch should store data for a given stream_id")
    processor.add_batch([sensor.stream_id], ["temp:22.5", "humidity:65"])
    print(" OK")
    print("=> add_batch should extend existing data for a given stream_id")
    processor.add_batch([sensor.stream_id], ["pressure:1013"])
    print(" OK")
    print("=> add_batch should handle multiple stream_ids at once")
    transaction = TransactionStream()
    event = EventStream()
    processor.add_batch(
        [transaction.stream_id, event.stream_id],
        ["buy:100"]
    )
    print(" OK")


def test_stream_processor_process_stream() -> None:
    print(" Testing process_stream method of StreamProcessor")
    processor = StreamProcessor()
    sensor = SensorStream()
    print(
        "=> process_stream should raise ValueError",
        "if no batch is associated"
    )
    try:
        processor.process_stream(sensor)
        assert False, "Should have raised ValueError on missing batch"
    except ValueError:
        pass
    print(" OK")
    print("=> process_stream should return stats after processing")
    processor.add_batch([sensor.stream_id], ["temp:22.5", "humidity:65"])
    stats = processor.process_stream(sensor)
    assert stats["total_readings"] == 2, \
        f"Expected total_readings=2, got {stats['total_readings']}"
    assert stats["temp_avg"] == 22.5, \
        f"Expected temp_avg=22.5, got {stats['temp_avg']}"
    print(" OK")
    print("=> process_stream with high_priority should filter data first")
    processor2 = StreamProcessor()
    sensor2 = SensorStream()
    processor2.add_batch(
        [sensor2.stream_id],
        ["temp:22.5", "temp:45.0", "humidity:65", "humidity:95.0"]
    )
    stats = processor2.process_stream(sensor2, high_priority=True)
    assert stats["total_readings"] == 2, (
        "Expected total_readings=2 after high_priority filter," +
        f" got {stats['total_readings']}"
    )
    print(" OK")
    print(
        "=> process_stream with high_priority and no matching data" +
        " should return empty stats"
    )
    processor3 = StreamProcessor()
    sensor3 = SensorStream()
    processor3.add_batch([sensor3.stream_id], ["temp:22.5", "humidity:65"])
    stats = processor3.process_stream(sensor3, high_priority=True)
    assert stats == {}, \
        f"Expected empty stats, got {stats}"
    print(" OK")


def test_stream_processor_process_mixed_stream() -> None:
    print(" Testing process_mixed_stream method of StreamProcessor")
    processor = StreamProcessor()
    sensor = SensorStream()
    transaction = TransactionStream()
    event = EventStream()
    processor.add_batch([sensor.stream_id], ["temp:22.5", "humidity:65"])
    processor.add_batch([transaction.stream_id], ["buy:100", "sell:150"])
    processor.add_batch([event.stream_id], ["login", "error", "logout"])
    print("=> process_mixed_stream should return stats for all streams")
    stats = processor.process_mixed_stream([sensor, transaction, event])
    assert sensor.stream_id in stats, \
        f"Expected {sensor.stream_id} in stats"
    assert transaction.stream_id in stats, \
        f"Expected {transaction.stream_id} in stats"
    assert event.stream_id in stats, \
        f"Expected {event.stream_id} in stats"
    print(" OK")
    print("=> process_mixed_stream should return correct stats per stream")
    assert stats[sensor.stream_id]["total_readings"] == 2, (
        "Expected total_readings=2, got" +
        f" {stats[sensor.stream_id]['total_readings']}"
    )
    assert stats[transaction.stream_id]["total_operations"] == 2, (
        "Expected total_operations=2, got" +
        " {stats[transaction.stream_id]['total_operations']}"
    )
    assert stats[event.stream_id]["total_events"] == 3, (
        "Expected total_events=3, got" +
        f" {stats[event.stream_id]['total_events']}"
    )
    print(" OK")
    print(
        "=> process_mixed_stream with high_priority",
        "should filter each stream"
    )
    processor2 = StreamProcessor()
    sensor2 = SensorStream()
    transaction2 = TransactionStream()
    event2 = EventStream()
    processor2.add_batch(
        [sensor2.stream_id],
        ["temp:22.5", "temp:45.0"]
    )
    processor2.add_batch(
        [transaction2.stream_id],
        ["buy:100", "sell:200"]
    )
    processor2.add_batch(
        [event2.stream_id],
        ["login", "error", "logout"]
    )
    stats = processor2.process_mixed_stream(
        [sensor2, transaction2, event2],
        high_priority=True
    )
    assert stats[sensor2.stream_id]["total_readings"] == 1, (
        "Expected total_readings=1, got" +
        f" {stats[sensor2.stream_id]['total_readings']}"
    )
    assert stats[transaction2.stream_id]["total_operations"] == 1, (
        "Expected total_operations=1, got" +
        f" {stats[transaction2.stream_id]['total_operations']}"
    )
    assert stats[event2.stream_id]["total_events"] == 1, (
        "Expected total_events=1, got" +
        f" {stats[event2.stream_id]['total_events']}"
    )
    print(" OK")


def test_stream_processor() -> None:
    print("Testing StreamProcessor")
    test_stream_processor_add_batch()
    test_stream_processor_process_stream()
    test_stream_processor_process_mixed_stream()
    print("StreamProcessor OK")


def run_tests() -> None:
    print("##### Running unit tests #####")
    print("=====")
    test_sensor_stream()
    print("=====")
    test_transaction_stream()
    print("=====")
    test_event_stream()
    print("=====")
    test_stream_processor()
    print("=====")
    print("##### All tests OK #####")


def sensor_stream_processor_demo() -> None:
    processor = StreamProcessor()
    print("\nInitializing Sensor Stream...")
    sensor_stream = SensorStream()
    print(
        f"Stream ID: {sensor_stream.stream_id},",
        f"Type: {sensor_stream.stream_type}"
    )
    sensor_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Processing sensor batch: {sensor_batch}")
    processor.add_batch([sensor_stream.stream_id], sensor_batch)
    stats: Dict[str, Union[str, int, float]] = processor.process_stream(
        sensor_stream
    )
    print(
        f"Sensor analysis: {stats['total_readings']} readings processed,",
        f"avg temp: {stats['temp_avg']:.1f}°C"
    )


def transaction_stream_processor_demo() -> None:
    processor = StreamProcessor()
    print("\nInitializing Transaction Stream...")
    transaction_stream = TransactionStream()
    print(
        f"Stream ID: {transaction_stream.stream_id},",
        f"Type: {transaction_stream.stream_type}"
    )
    transaction_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {transaction_batch}")
    processor.add_batch([transaction_stream.stream_id], transaction_batch)
    stats: Dict[str, Union[str, int, float]] = processor.process_stream(
        transaction_stream
    )
    print(
        f"Transaction analysis: {stats['total_operations']} operations,",
        f"net flow: {stats['net_flow']} units"
    )


def event_stream_demo() -> None:
    processor = StreamProcessor()
    print("\nInitializing Event Stream...")
    event_stream = EventStream()
    print(
        f"Stream ID: {event_stream.stream_id},",
        f"Type: {event_stream.stream_type}"
    )
    events_batch = ["login", "error", "logout"]
    print(f"Processing transaction batch: {events_batch}")
    processor.add_batch([event_stream.stream_id], events_batch)
    stats: Dict[str, Union[str, int, float]] = processor.process_stream(
        event_stream
    )
    print(
        f"Event analysis: {stats['total_events']} events,",
        f"{stats['error_count']} error detected"
    )


"""
"""


def polymorphic_stream_processor_demo() -> None:
    processor = StreamProcessor()
    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")

    sensor_stream = SensorStream()
    transaction_stream = TransactionStream()
    event_stream = EventStream()

    mixed_batch: List[str] = [
        "buy:1312",
        "temp:420",
        "login",
        "sell:69",
        "humidity:1030",
        "buy:42",
        "logout",
        "buy:69",
        "login"
    ]

    processor.add_batch([
        sensor_stream.stream_id,
        transaction_stream.stream_id,
        event_stream.stream_id
    ], mixed_batch)

    stats = processor.process_mixed_stream([
        sensor_stream,
        transaction_stream,
        event_stream
    ])
    print(stats)
    print(" Batch 1 Results:")
    print(f"- Sensor data: {stats[
        sensor_stream.stream_id
    ][
        'total_readings'
    ]} readings processed")
    print(
        f"- Transaction data: {stats[
            transaction_stream.stream_id
        ][
            'total_operations'
        ]}",
        "operations processed"
    )
    print(f"- Event data: {stats[
        event_stream.stream_id
    ][
        'total_events'
    ]} events processed")

    print("\nStream filtering active: High-priority data only")
    stats = processor.process_mixed_stream([
        sensor_stream,
        transaction_stream,
        event_stream,
    ], high_priority=True)
    print(f"Filtered results: {stats[
        sensor_stream.stream_id
    ][
        'total_readings'
    ]} critical sensor alerts, {stats[
        transaction_stream.stream_id
    ][
        'total_operations'
    ]} large transaction")


def stream_processor_demo() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    sensor_stream_processor_demo()
    transaction_stream_processor_demo()
    event_stream_demo()
    polymorphic_stream_processor_demo()


def main() -> None:
    # run_tests()
    stream_processor_demo()


if __name__ == "__main__":
    main()
