#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Type, Union


class DataProcessorUtils:

    @staticmethod
    def isinstance_lst(lst: List[Any], expected_type: Type) -> bool:
        for el in lst:
            if not isinstance(el, expected_type):
                return False
        return True

    @staticmethod
    def avg(data: List[int]) -> float:
        return sum(data) / len(data)


class DataProcessor(ABC):

    def __init__(_) -> None:
        super().__init__()

    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return result


class NumericProcessor(DataProcessor):

    def __init__(_) -> None:
        super().__init__()
        print("Initializing Numeric Processor...")

    def process(self, data: List[int]) -> str:
        if not self.validate(data):
            raise ValueError(f"Cannot process invalid numeric data {data}")
        return (
            f"{len(data)}||{sum(data)}||{DataProcessorUtils.avg(data):.1f}"
        )

    def validate(self, data: List[int]) -> bool:
        return (
            isinstance(data, List)
            and DataProcessorUtils.isinstance_lst(data, int)
            and len(data) > 0
        )

    def format_output(self, result: str) -> str:
        splitted_result = result.split("||")
        if len(splitted_result) != 3:
            raise ValueError(f"Invalid result for numeric processor: {result}")
        data_len = splitted_result[0]
        data_sum = splitted_result[1]
        data_avg = splitted_result[2]
        return (
            f"Processed {data_len} numeric values, sum={data_sum}," +
            f"avg={data_avg}"
        )


class TextProcessor(DataProcessor):

    def __init__(_) -> None:
        super().__init__()
        print("Initializing Text Processor...")

    def process(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError(f"Cannot process invalid text data \"{data}\"")
        data_length = len(data)
        data_words_count = len(data.split())
        return (
            f"{data_length}||{data_words_count}"
        )

    def validate(self, data: str) -> bool:
        return isinstance(data, str) and data != "" and "||" not in data

    def format_output(self, result: str) -> str:
        splitted_result = result.split("||")
        if len(splitted_result) != 2:
            raise ValueError(f"Invalid result for text processor: {result}")
        data_length = splitted_result[0]
        data_words_count = splitted_result[1]
        return (
            f"Processed text: {data_length} characters, "
            + f"{data_words_count} words"
        )


class LogProcessor(DataProcessor):
    __log_levels: List[str] = [
        "DEBUG",
        "INFO",
        "WARN",
        "ERROR"
    ]

    def __init__(_) -> None:
        super().__init__()
        print("Initializing Log Processor...")

    def process(self, data: str) -> str:
        if not self.validate(data):
            raise ValueError(f"Cannot process invalid log data \"{data}\"")
        log_split = data.split(": ")
        log_level = log_split[0]
        log_message = log_split[1]
        output_marker: str
        if log_level in ["WARN", "ERROR"]:
            output_marker = "[ALERT]"
        else:
            output_marker = "[INFO]"
        return f"{output_marker}||{log_level}||{log_message}"

    def validate(self, data: str) -> bool:
        if not isinstance(data, str):
            return False
        log_split = data.split(": ")
        if len(log_split) != 2:
            return False
        log_level = log_split[0]
        log_message = log_split[1]
        if log_message.split() == []:
            return False
        return (
            log_level in self.__log_levels
            and log_message != ""
            and "||" not in log_message
        )

    def format_output(self, result: str) -> str:
        splitted_result = result.split("||")
        if len(splitted_result) != 3:
            raise ValueError(f"Invalid result for log processor: {result}")
        output_marker = splitted_result[0]
        log_level = splitted_result[1]
        log_message = splitted_result[2]
        return f"{output_marker} {log_level} level detected: {log_message}"


def validate_numeric_data(
        processor: NumericProcessor,
        data: List[int]
) -> None:
    if processor.validate(data):
        print("Validation: Numeric data verified")
    else:
        raise ValueError(f"Invalid data for numeric processor: {data}")


def validate_text_data(processor: TextProcessor, data: str) -> None:
    if processor.validate(data):
        print("Validation: Text data verified")
    else:
        raise ValueError(f"Invalid data for text processor: {data}")


def validate_log_data(processor: LogProcessor, data: str) -> None:
    if processor.validate(data):
        print("Validation: Log data verified")
    else:
        raise ValueError(f"Invalid data for log processor: {data}")


def test_processor(
    data: Any,
    type: str,
    processor: DataProcessor
) -> None:
    print(f"Processing data: {data}")
    if type == "text":
        validate_text_data(processor, data)
    elif type == "numeric":
        validate_numeric_data(processor, data)
    elif type == "log":
        validate_log_data(processor, data)
    else:
        raise ValueError(f"Invalid data type: {type}")
    print(
        "Output:",
        processor.format_output(
            processor.process(data)
        )
    )


def main() -> None:
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")

    processors: dict[str, DataProcessor] = {
        "numeric": NumericProcessor(),
        "text": TextProcessor(),
        "log": LogProcessor()
    }

    try:
        print("")
        test_processor(
            [1, 2, 3, 4, 5],
            "numeric",
            processors.get("numeric")
        )
        print("")
        test_processor(
            "Hello Nexus World",
            "text",
            processors.get("text")
        )
        print("")
        test_processor(
            "ERROR: Connection timeout",
            "log",
            processors.get("log")
        )
    except ValueError as err:
        print(f"A value error occured when processing the data : {err}")

    print("\n=== Polymorphic Processing Demo ===\n")

    data_list: List[Union[List[int], str]] = [
        [1, 2, 3],
        "Lorem ipsumm",
        "INFO: Quoicoubeh !"
    ]
    i = 1

    for data in data_list:
        processor: DataProcessor

        for processor in [
            processors.get("log"),
            processors.get("text"),
            processors.get("numeric")
        ]:
            if processor.validate(data):
                break
        print(
            f"Result {i}:",
            processor.format_output(
                processor.process(
                    data
                )
            )
        )
        i += 1

    print("\nFoundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    main()
