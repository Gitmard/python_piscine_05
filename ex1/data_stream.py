#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Optional, Dict, Union


class DataStream(ABC):

    stream_id: str
    __stats: Dict[str, Union[str, int, float]]
    __tokens: List[str]

    def __init__(self, stream_id: str, tokens: List[str] = []) -> None:
        self.stream_id = stream_id
        self.__stats = {}
        self.__tokens = tokens

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
        return self.__stats

    def can_handle(self, data: Any) -> bool:
        data_str = f"{data}"
        splitted_data = data_str.split(":")
        return splitted_data[0] in self.__tokens


class SensorStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, tokens=[
            "temp",
            "humidity",
            "pressure"
        ])

    def process_batch(self, data_batch: List[Any]) -> str:
        return super().process_batch(data_batch)

    def can_handle(self, data: str) -> bool:
        splitted_data: List[str]
        data_value_is_float: bool

        if not super().can_handle(data):
            return False
        splitted_data = data.split(":")
        try:
            float(splitted_data[1])
            data_value_is_float = True
        except ValueError:
            data_value_is_float = False
        return data_value_is_float


class TransactionStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        return super().process_batch(data_batch)


class EventStream(DataStream):

    def process_batch(self, data_batch: List[Any]) -> str:
        return super().process_batch(data_batch)


def test_sensor_stream_can_handle() -> None:
    print(" Testing can_handle method of SensorStream")
    stream = SensorStream(42)
    print(
        "=> SensorStream should handle",
        "[valid token]:[valid number]"
    )
    assert stream.can_handle("temp:42")
    assert stream.can_handle("humidity:42")
    assert stream.can_handle("pressure:42")
    print(" OK")
    print(
        "=> SensorStream should handle",
        "[valid token]:[valid float number]"
    )
    assert stream.can_handle("temp:42.42")
    assert stream.can_handle("humidity:42.42")
    assert stream.can_handle("pressure:42.42")
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[valid token]:[invalid number]"
    )
    assert not stream.can_handle("temp:abc")
    assert not stream.can_handle("humidity:abc")
    assert not stream.can_handle("pressure:abc")
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[invalid token]:[valid number]"
    )
    assert not stream.can_handle("abc:42")
    print(" OK")
    print(
        "=> SensorStream should not handle",
        "[invalid token]:[invalid number]"
    )
    assert not stream.can_handle("abc:abc")
    print(" OK")


def test_sensor_stream() -> None:
    print("Testing SensorStream")
    test_sensor_stream_can_handle()
    print("SensorStream OK")


def run_tests() -> None:
    print("##### Running unit tests #####")
    print("=====")
    test_sensor_stream()
    print("=====")
    print("##### All tests OK #####")


def main() -> None:
    run_tests()


if __name__ == "__main__":
    main()
