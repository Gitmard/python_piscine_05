#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):

    stream_id: str
    _stats: Dict[str, Union[str, int, float]]
    _keys: List[str]

    def __init__(
        self,
        stream_id: str,
        keys: Optional[List[str]] = None
    ) -> None:
        self.stream_id = stream_id
        self._stats = {}
        if keys is None:
            keys = []
        self._keys = keys

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
        self,
        data_batch: List[Any],
        criteria: Optional[str] = None
    ) -> List[Any]:
        return ([
            item for item in data_batch
            if f"{item}" != f"{criteria}"
        ])

    def get_stats(
        self
    ) -> Dict[str, Union[str, int, float]]:
        return self._stats

    @abstractmethod
    def can_handle(self, data: Any) -> bool:
        pass


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        print("Initializing Sensor Stream...")
        super().__init__(stream_id, keys=[
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
            self.__update_stats("temp_avg", temp_sum / temp_readings_count)
        self.__update_stats("total_readings", batch_len)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        splitted_data: List[str]

        if not isinstance(data, str):
            return False
        splitted_data = data.split(":")
        if splitted_data[0] not in self._keys:
            return False
        try:
            float(splitted_data[1])
        except ValueError:
            return False
        return True

    def __update_stats(self, key: str, value: float) -> None:
        old_value = self._stats.get(key) or 0
        self._stats[key] = old_value + value


class TransactionStream(DataStream):

    def __init__(self, stream_id: str):
        print("Initializing Transaction Stream...")
        super().__init__(stream_id, [
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
                raise ValueError(f"SensorStream cannot handle {data}")
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
        self.__update_stats("total_operations", batch_len)
        self.__update_stats("net_flow", net_flow)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        splitted_data: List[str]

        if not isinstance(data, str):
            return False
        splitted_data = data.split(":")
        if splitted_data[0] not in self._keys:
            return False
        try:
            int(splitted_data[1])
        except ValueError:
            return False
        return True

    def __update_stats(self, key: str, value: float) -> None:
        old_value = self._stats.get(key) or 0
        self._stats[key] = old_value + value


class EventStream(DataStream):

    def __init__(self, stream_id: str):
        print("Initializing Event Stream...")
        super().__init__(stream_id, [
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
                raise ValueError(f"SensorStream cannot handle {data}")
            processed_batch += f"{data}"
            i += 1
            if i != batch_len:
                processed_batch += ", "
            if data == "error":
                error_count += 1
        processed_batch += "]"
        self.__update_stats("total_events", batch_len)
        self.__update_stats("error_count", error_count)
        return processed_batch

    def can_handle(self, data: str) -> bool:
        if not isinstance(data, str):
            return False
        if data not in self._keys:
            return False
        return True

    def __update_stats(self, key: str, value: float) -> None:
        old_value = self._stats.get(key) or 0
        self._stats[key] = old_value + value


def test_sensor_stream_can_handle() -> None:
    print(" Testing can_handle method of SensorStream")
    stream = SensorStream("42")
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
    stream = SensorStream("42")
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
    assert stats["temp_avg"] == 47.0, \
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


def test_sensor_stream() -> None:
    print("Testing SensorStream")
    test_sensor_stream_can_handle()
    test_sensor_stream_process_batch()
    print("SensorStream OK")


def test_transaction_stream_can_handle() -> None:
    print(" Testing can_handle method of TransactionStream")
    stream = TransactionStream("42")
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
    stream = TransactionStream("42")
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
    assert stats["net_flow"] == -25, \
        f"Expected net_flow=-25, got {stats['net_flow']}"
    print(" OK")
    print("=> process_batch should accumulate stats across multiple calls")
    stream.process_batch(["sell:200", "buy:50"])
    stats = stream.get_stats()
    assert stats["total_operations"] == 5, \
        f"Expected total_operations=5, got {stats['total_operations']}"
    assert stats["net_flow"] == 125, \
        f"Expected net_flow=125, got {stats['net_flow']}"
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


def test_transaction_stream() -> None:
    print("Testing TransactionStream")
    test_transaction_stream_can_handle()
    test_transaction_stream_process_batch()
    print("TransactionStream OK")


def test_event_stream_can_handle() -> None:
    print(" Testing can_handle method of EventStream")
    stream = EventStream("42")
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
    stream = EventStream("42")
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
        assert False, "Should have raised ValueError on unknown event 'transfer'"
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


def test_event_stream() -> None:
    print("Testing EventStream")
    test_event_stream_can_handle()
    test_event_stream_process_batch()
    print("EventStream OK")


def run_tests() -> None:
    print("##### Running unit tests #####")
    print("=====")
    test_sensor_stream()
    print("=====")
    test_transaction_stream()
    print("=====")
    test_event_stream()
    print("=====")
    print("##### All tests OK #####")

def main() -> None:
    run_tests()


if __name__ == "__main__":
    main()
