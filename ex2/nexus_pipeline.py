#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Dict, List, Protocol, Any, TypedDict, Union


class ProcessingStagePayload(TypedDict):
    pass


class SensorProcessingStagePayload(ProcessingStagePayload):
    sensor: str
    value: float
    unit: str


class SensorListProcessingStagePayload(ProcessingStagePayload):
    readings: List[SensorProcessingStagePayload]


class UserActivityLog(TypedDict):
    user: str
    action: str
    timestamp: str


class UserActivityProcessingStagePayload(ProcessingStagePayload):
    logs: List[UserActivityLog]


class ProcessingStageMetadata(TypedDict):
    pass


class SensorProcessingStageMetadata(ProcessingStageMetadata):
    temperature_range: str


class UserActivityProcessingStageMetadata(ProcessingStageMetadata):
    actions_count: int


class SensorListProcessingStageMetadata(ProcessingStageMetadata):
    readings_count: int
    avg_temp: float


class ProcessingStageDict(TypedDict):
    type: str
    payload: ProcessingStagePayload
    metadata: ProcessingStageMetadata


class ProcessingStage(Protocol):

    def process(self, data: Any) -> Any:
        ...


class PipelineError(ValueError):
    pass


class InputStage:

    def __process_sensor_data(
            self,
            data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: SensorProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        if (
            not isinstance(payload.get("sensor"), str)
            or (
                not isinstance(payload.get("value"), float)
                and not isinstance(payload.get("value"), int)
            )
            or not isinstance(payload.get("unit"), str)
            or not len(payload.get("sensor") or "")
            or not len(payload.get("unit") or "")
        ):
            raise PipelineError(
                "Error: invalid sensor data"
            )
        return data

    def __process_user_activity_data(
        self,
        data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: UserActivityProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        for log in payload["logs"]:
            if (
                not isinstance(log.get("user"), str)
                or not isinstance(log.get("action"), str)
                or not isinstance(log.get("timestamp"), str)
                or not len(log.get("user") or "")
                or not len(log.get("action") or "")
                or not len(log.get("timestamp") or "")
            ):
                raise PipelineError(
                    "Error: invalid user activity data"
                )
        return data

    def __process_sensor_list(
        self,
        data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: SensorListProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        for sensor_reading in payload["readings"]:
            self.__process_sensor_data(
                {"payload": sensor_reading}  # type: ignore
            )
        return data

    def process(self, data: Any) -> ProcessingStageDict:
        if data is None:
            raise PipelineError("Data is None")
        elif not isinstance(data, Dict):
            raise PipelineError("Data must be a dict")
        if data["type"] == "sensor":
            return self.__process_sensor_data(
                data  # pyright: ignore[reportArgumentType]
            )
        elif data["type"] == "user_activity":
            return self.__process_user_activity_data(
                data  # pyright: ignore[reportArgumentType]
            )
        elif data["type"] == "sensor_list":
            return self.__process_sensor_list(
                data  # pyright: ignore[reportArgumentType]
            )
        else:
            raise PipelineError(f"Unhandled data type {data['type']}")


class TransformStage:

    def __process_sensor_data(
        self,
        data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: SensorProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        data["metadata"] = {}
        metadata: SensorProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]
        if payload["value"] < 0:
            metadata["temperature_range"] = "Cold range"
        elif payload["value"] < 25:
            metadata["temperature_range"] = "Normal range"
        else:
            metadata["temperature_range"] = "Hot range"
        return data

    def __process_user_activity_data(
        self,
        data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: UserActivityProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        data["metadata"] = {}
        metadata: UserActivityProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]
        metadata["actions_count"] = len(payload["logs"])
        return data

    def __process_sensor_data_list(
        self,
        data: ProcessingStageDict
    ) -> ProcessingStageDict:
        payload: SensorListProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        data["metadata"] = {}
        metadata: SensorListProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]
        metadata["readings_count"] = len(payload["readings"])
        temp_sum = 0.0
        for reading in payload["readings"]:
            temp_sum += reading["value"]
        metadata["avg_temp"] = temp_sum / len(payload["readings"])
        return data

    def process(self, data: ProcessingStageDict) -> ProcessingStageDict:
        if data["type"] == "sensor":
            return self.__process_sensor_data(data)
        elif data["type"] == "user_activity":
            return self.__process_user_activity_data(data)
        else:
            return self.__process_sensor_data_list(data)


class OutputStage:

    def __process_user_activity_data(
        self,
        data: ProcessingStageDict
    ) -> str:
        metadata: UserActivityProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]
        return (
            f"User activity logged: {metadata.get('actions_count')}" +
            " actions processed"
        )

    def __process_sensor_data(
        self,
        data: ProcessingStageDict
    ) -> str:
        payload: SensorProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        metadata: SensorProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]

        return (
            f"Processed temperature reading: {payload.get('value')}°" +
            f"{payload['unit']}" +
            f" ({metadata.get('temperature_range')})"
        )

    def __process_sensor_list_data(self, data: ProcessingStageDict) -> str:
        payload: SensorListProcessingStagePayload = data[
            "payload"
        ]  # pyright: ignore[reportAssignmentType]
        metadata: SensorListProcessingStageMetadata = data[
            "metadata"
        ]  # pyright: ignore[reportAssignmentType]
        return (
            f"Stream summary: {metadata.get('readings_count')} readings," +
            f" avg: {(metadata.get('avg_temp') or 0):.1f}°" +
            f" {payload['readings'][0]['unit']}"
        )

    def process(self, data: ProcessingStageDict) -> str:
        if data["type"] == "sensor":
            return self.__process_sensor_data(data)
        elif data["type"] == "user_activity":
            return self.__process_user_activity_data(data)
        return self.__process_sensor_list_data(data)


class ProcessingPipeline(ABC):

    __static_last_id = 0

    pipeline_id: str
    stages: List[ProcessingStage]

    def __init__(self, pipeline_id: str = "PIPELINE") -> None:
        ProcessingPipeline.__static_last_id += 1
        self.pipeline_id = (
            f"{pipeline_id}_" +
            f"{ProcessingPipeline.__static_last_id:03}"
        )
        self.stages = []

    def add_stage(self, stage: ProcessingStage) -> None:
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Union[str, Any]:
        pass

    def _is_processed(self, data: Any) -> bool:
        if not isinstance(data, Dict):
            return False
        if not data.get("type"):
            return False
        if data.get("type") == "sensor":
            if (
                data.get("payload") is None
                or data.get("metadata") is None
                or data["payload"].get("sensor") is None
                or data["payload"].get("value") is None
                or data["payload"].get("unit") is None
            ):
                raise PipelineError("Corrupted sensor data")
            return True
        elif data.get("type") == "user_activity":
            if (
                data.get("payload") is None
                or data.get("metadata") is None
            ):
                raise PipelineError("Corrupted user activity data")
            for log in data["payload"]["logs"]:
                if (
                    log.get("user") is None
                    or log.get("action") is None
                    or log.get("timestamp") is None

                ):
                    raise PipelineError("Corrupted user activity data")
            return True
        elif data.get("type") == "sensor_list":
            if (
                data.get("metadata") is None
                or data.get("payload") is None
                or data["payload"].get("readings") is None
            ):
                raise PipelineError("Corrupted readings list data")
            for reading in data["payload"]["readings"]:
                if (
                    reading.get("sensor") is None
                    or reading.get("value") is None
                    or reading.get("unit") is None
                ):
                    raise PipelineError("Corrupted readings list data")
            return True
        raise PipelineError("Invalid pipeline data type")


class JSONAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str = "JSON_ADAPTER") -> None:
        super().__init__(pipeline_id)

    def __adapt_data(self, data: Any) -> ProcessingStageDict:
        adapted_data: ProcessingStageDict = {}  # type: ignore
        if not isinstance(data, dict) and not isinstance(data, List):
            raise PipelineError("Invalid JSON Data")
        if (
            isinstance(data, Dict)
            and data.get("sensor") is not None
            and data.get("value") is not None
            and data.get("unit") is not None
        ):
            adapted_data["type"] = "sensor"
        elif isinstance(data, List) and len(data) > 0:
            first = data[0]
            if (
                isinstance(first, Dict)
                and first.get("user") is not None
                and first.get("action") is not None
                and first.get("timestamp") is not None
            ):
                adapted_data["type"] = "user_activity"
                for el in data:
                    if (
                        el.get("user") is None
                        or el.get("action") is None
                        or el.get("timestamp") is None
                    ):
                        raise PipelineError("Error: Invalid JSON Data")
            elif (
                isinstance(first, Dict)
                and first.get("sensor") is not None
                and first.get("value") is not None
                and first.get("unit") is not None
            ):
                adapted_data["type"] = "sensor_list"
                for el in data:
                    if (
                        el.get("sensor") is None
                        or el.get("value") is None
                        or el.get("unit") is None
                    ):
                        raise PipelineError("Error: Invalid JSON Data")
            else:
                raise PipelineError("Error: Invalid JSON Data")
        else:
            raise PipelineError("Error: Invalid JSON Data")
        if adapted_data["type"] == "sensor":
            adapted_data["payload"] = data  # type: ignore
        elif adapted_data["type"] == "user_activity":
            adapted_data["payload"] = {"logs": data}  # type: ignore
        elif adapted_data["type"] == "sensor_list":
            adapted_data["payload"] = {"readings": data}  # type: ignore
        adapted_data["metadata"] = {}
        return adapted_data

    def process(self, data: Any) -> Union[str, Any]:
        if not self._is_processed(data):
            data = self.__adapt_data(data)
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class CSVAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str = "CSV_ADAPTER") -> None:
        super().__init__(pipeline_id)

    def __adapt_data(self, data: str) -> ProcessingStageDict:
        adapted_data: ProcessingStageDict = {
            "type": "user_activity"
        }  # type: ignore
        lines = data.split("\n")
        payload: ProcessingStagePayload = {
            "logs": []  # type: ignore
        }
        metadata: ProcessingStageMetadata = {}

        for line in lines:
            log = {}
            keys = line.split(",")
            if len(keys) != 3:
                raise PipelineError("Error: Invalid CSV Data")
            log["user"] = keys[0]
            log["action"] = keys[1]
            log["timestamp"] = keys[2]
            payload["logs"].append(log)  # type: ignore
        adapted_data["payload"] = payload
        adapted_data["metadata"] = metadata
        return adapted_data

    def process(self, data: Any) -> Union[str, Any]:
        if not self._is_processed(data):
            data = self.__adapt_data(data)
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class StreamAdapter(ProcessingPipeline):

    def __init__(self, pipeline_id: str = "STREAM_ADAPTER") -> None:
        super().__init__(pipeline_id)

    def __adapt_data(self, data: Any) -> ProcessingStageDict:
        adapted_data: ProcessingStageDict = {
            "type": "sensor_list",
            "payload": {},
            "metadata": {}
        }
        if not isinstance(data, List):
            raise PipelineError("Error: Invalid Stream Data")
        adapted_data["payload"]["readings"] = []  # type: ignore
        for sensor in data:
            if (
                not isinstance(sensor, Dict)
                or sensor.get("sensor") is None
                or sensor.get("value") is None
                or sensor.get("unit") is None
            ):
                raise PipelineError("Error: Invalid Stream Data")
            adapted_data["payload"]["readings"].append(sensor)  # type: ignore
        return adapted_data

    def process(self, data: Any) -> Union[str, Any]:
        if not self._is_processed(data):
            data = self.__adapt_data(data)
        result = data
        for stage in self.stages:
            result = stage.process(result)
        return result


class NexusManager:
    __pipelines: List[ProcessingPipeline]

    def __init__(self) -> None:
        self.__pipelines = []

    def add_pipeline(self, pipeline: ProcessingPipeline) -> None:
        self.__pipelines.append(pipeline)

    def get_pipeline_by_id(self, pipeline_id: str) -> ProcessingPipeline:
        pipeline = [
            pipeline for pipeline in self.__pipelines
            if pipeline.pipeline_id == pipeline_id
        ]
        if not len(pipeline):
            raise PipelineError(f"Unknown pipeline id {pipeline_id}")
        elif len(pipeline) > 1:
            print(
                f"WARNING: id {pipeline_id} corresponds to",
                "multiple pipelines, using the first one..."
            )
        return pipeline[0]

    def process(self, pipeline_id: str, data: Any) -> Any:
        pipeline = self.get_pipeline_by_id(pipeline_id)
        result: Any = None
        try:
            result = pipeline.process(data)
        except PipelineError as err:
            print(
                f"Error when processing data with pipeline_id {pipeline_id}",
                err
            )
            raise err
        return result

    def process_chained_pipelines(
            self,
            pipeline_ids: List[str],
            data: Any
    ) -> Any:
        pipelines: List[ProcessingPipeline] = []
        for pipeline_id in pipeline_ids:
            pipelines.append(self.get_pipeline_by_id(pipeline_id))
        result = data
        for pipeline in pipelines:
            try:
                result = pipeline.process(result)
            except PipelineError as err:
                print(
                    f"Error when chaining pipelines {pipeline_ids}",
                    err
                )
                raise err
        return result

    def process_data(self) -> None:
        input_stage = InputStage()
        transform_stage = TransformStage()
        output_stage = OutputStage()

        print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")

        print("Initializing Nexus Manager...")
        print("Pipeline capacity: 1000 streams/second\n")

        print("Creating Data Processing Pipeline...")
        print("Stage 1: Input validation and parsing")
        print("Stage 2: Data transformation and enrichment")
        print("Stage 3: Output formatting and delivery")

        json_pipeline = JSONAdapter()
        csv_pipeline = CSVAdapter()
        stream_pipeline = StreamAdapter()

        for pipeline in [json_pipeline, csv_pipeline, stream_pipeline]:
            pipeline.add_stage(input_stage)
            pipeline.add_stage(transform_stage)
            pipeline.add_stage(output_stage)
            self.add_pipeline(pipeline)

        print("\n=== Multi-Format Data Processing ===")

        print("\nProcessing JSON data through pipeline...")
        json_data = {"sensor": "temp", "value": 23.5, "unit": "C"}
        print(f"Input: {json_data}")
        print("Transform: Enriched with metadata and validation")
        result = self.process(json_pipeline.pipeline_id, json_data)
        print(f"Output: {result}")

        print("\nProcessing CSV data through same pipeline...")
        csv_data = "user,action,timestamp"
        print(f"Input: \"{csv_data}\"")
        print("Transform: Parsed and structured data")
        result = self.process(csv_pipeline.pipeline_id, csv_data)
        print(f"Output: {result}")

        print("\nProcessing Stream data through same pipeline...")
        stream_data = [
            {"sensor": "temp", "value": 20.0, "unit": "C"},
            {"sensor": "temp", "value": 22.5, "unit": "C"},
            {"sensor": "temp", "value": 21.0, "unit": "C"},
            {"sensor": "temp", "value": 23.5, "unit": "C"},
            {"sensor": "temp", "value": 23.5, "unit": "C"},
        ]
        print("Input: Real-time sensor stream")
        print("Transform: Aggregated and filtered")
        result = self.process(stream_pipeline.pipeline_id, stream_data)
        print(f"Output: {result}")

        print("\n=== Pipeline Chaining Demo ===")
        pipeline_a = JSONAdapter()
        pipeline_b = JSONAdapter()
        pipeline_c = JSONAdapter()

        pipeline_a.add_stage(input_stage)
        pipeline_b.add_stage(transform_stage)
        pipeline_c.add_stage(output_stage)

        for pipeline in [pipeline_a, pipeline_b, pipeline_c]:
            self.add_pipeline(pipeline)

        print(
            f"Pipeline A ({pipeline_a.pipeline_id})",
            f"-> Pipeline B ({pipeline_b.pipeline_id})",
            f"-> Pipeline C ({pipeline_c.pipeline_id})"
        )
        print("Data flow: Raw -> Processed -> Analyzed -> Stored\n")

        result = self.process_chained_pipelines(
            [
                pipeline_a.pipeline_id,
                pipeline_b.pipeline_id,
                pipeline_c.pipeline_id
            ],
            {"sensor": "temp", "value": 23.5, "unit": "C"}
        )
        print(f"Chain result: {result}")
        print("Performance: 95% efficiency, 0.2s total processing time")

        print("\n=== Error Recovery Test ===")
        print("Simulating pipeline failure...")
        backup_pipeline = JSONAdapter()
        backup_pipeline.add_stage(input_stage)
        backup_pipeline.add_stage(transform_stage)
        backup_pipeline.add_stage(output_stage)
        self.add_pipeline(backup_pipeline)

        try:
            self.process(
                json_pipeline.pipeline_id,
                {"invalid": "data"}
            )
        except PipelineError as e:
            print(f"Error detected in Stage 2: {e}")
            print("Recovery initiated: Switching to backup processor")
            result = self.process(
                backup_pipeline.pipeline_id,
                {"sensor": "temp", "value": 23.5, "unit": "C"}
            )
            print("Recovery successful: Pipeline restored, processing resumed")
            print(f"Output: {result}")

        print("\nNexus Integration complete. All systems operational.")


def test_json_adapter() -> None:
    print("=== JSON Adapter ===")
    input_stage = InputStage()
    transform_stage = TransformStage()
    output_stage = OutputStage()
    json_adapter = JSONAdapter()
    json_adapter.add_stage(input_stage)
    json_adapter.add_stage(transform_stage)
    json_adapter.add_stage(output_stage)

    print("\n-- Valid sensor data --")
    result = json_adapter.process({
        "sensor": "temp", "value": 23.5, "unit": "C"
    })
    assert result == "Processed temperature reading: 23.5°C (Normal range)", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    result = json_adapter.process({
        "sensor": "temp", "value": -5.0, "unit": "C"
    })
    assert result == "Processed temperature reading: -5.0°C (Cold range)", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    result = json_adapter.process({
        "sensor": "temp", "value": 42.0, "unit": "C"
    })
    assert result == "Processed temperature reading: 42.0°C (Hot range)", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Valid user activity data --")
    result = json_adapter.process([
        {"user": "user1", "action": "login", "timestamp": "42"},
        {"user": "user1", "action": "logout", "timestamp": "4269"}
    ])
    assert result == "User activity logged: 2 actions processed", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Valid sensor list data --")
    result = json_adapter.process([
        {"sensor": "temp", "value": 20.0, "unit": "C"},
        {"sensor": "temp", "value": 21.5, "unit": "C"},
        {"sensor": "temp", "value": 22.0, "unit": "C"}
    ])
    assert result == "Stream summary: 3 readings, avg: 21.2°C", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Already processed data --")
    result = json_adapter.process({
        "type": "sensor",
        "payload": {"sensor": "temp", "value": 23.5, "unit": "C"},
        "metadata": {}
    })
    assert result == "Processed temperature reading: 23.5°C (Normal range)", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Invalid JSON data --")
    for invalid in [
        "not a dict or list",
        42,
        {"unknown_key": "value"},
        [],
        [{"invalid": "element"}]
    ]:
        try:
            json_adapter.process(invalid)
            assert False, \
                f"Should have raised PipelineError for {invalid}"
        except PipelineError as e:
            print(f"OK - PipelineError raised: {e}")

    print("\n-- Invalid sensor data --")
    for invalid in [
        {"sensor": "", "value": 23.5, "unit": "C"},
        {"sensor": "temp", "value": "not_a_number", "unit": "C"},
        {"sensor": "temp", "value": 23.5, "unit": ""},
    ]:
        try:
            json_adapter.process(invalid)
            assert False, \
                f"Should have raised PipelineError for {invalid}"
        except PipelineError as e:
            print(f"OK - PipelineError raised: {e}")

    print("JSON Adapter OK")


def test_csv_adapter() -> None:
    print("=== CSV Adapter ===")
    input_stage = InputStage()
    transform_stage = TransformStage()
    output_stage = OutputStage()
    csv_adapter = CSVAdapter()
    csv_adapter.add_stage(input_stage)
    csv_adapter.add_stage(transform_stage)
    csv_adapter.add_stage(output_stage)

    print("\n-- Valid user activity data --")
    result = csv_adapter.process("user1,login,42\nuser1,logout,4269")
    assert result == "User activity logged: 2 actions processed", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    result = csv_adapter.process("user2,login,100")
    assert result == "User activity logged: 1 actions processed", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Invalid CSV data --")
    for invalid in [
        "user1,login",
        "user1,login,42,extra_column",
    ]:
        try:
            csv_adapter.process(invalid)
            assert False, \
                f"Should have raised PipelineError for '{invalid}'"
        except PipelineError as e:
            print(f"OK - PipelineError raised: {e}")

    print("CSV Adapter OK")


def test_stream_adapter() -> None:
    print("=== Stream Adapter ===")
    input_stage = InputStage()
    transform_stage = TransformStage()
    output_stage = OutputStage()
    stream_adapter = StreamAdapter()
    stream_adapter.add_stage(input_stage)
    stream_adapter.add_stage(transform_stage)
    stream_adapter.add_stage(output_stage)

    print("\n-- Valid sensor list data --")
    result = stream_adapter.process([
        {"sensor": "temp", "value": 20.0, "unit": "C"},
        {"sensor": "temp", "value": 22.5, "unit": "C"},
        {"sensor": "temp", "value": 21.0, "unit": "C"},
        {"sensor": "temp", "value": 23.5, "unit": "C"},
        {"sensor": "temp", "value": 22.3, "unit": "C"}
    ])
    assert result == "Stream summary: 5 readings, avg: 21.9°C", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Already processed sensor list --")
    result = stream_adapter.process({
        "type": "sensor_list",
        "payload": {
            "readings": [
                {"sensor": "temp", "value": 20.0, "unit": "C"},
                {"sensor": "temp", "value": 22.5, "unit": "C"}
            ]
        },
        "metadata": {}
    })
    assert result == "Stream summary: 2 readings, avg: 21.2°C", \
        f"Unexpected result: {result}"
    print(f"OK: {result}")

    print("\n-- Invalid stream data --")
    for invalid in [
        "not a list",
        42,
        [{"sensor": "temp", "value": 20.0}],
        [{"unknown": "data"}],
    ]:
        try:
            stream_adapter.process(invalid)
            assert False, \
                f"Should have raised PipelineError for {invalid}"
        except PipelineError as e:
            print(f"OK - PipelineError raised: {e}")

    print("Stream Adapter OK")


def run_tests() -> None:
    test_json_adapter()
    test_csv_adapter()
    test_stream_adapter()


def main() -> None:
    manager = NexusManager()
    manager.process_data()


if __name__ == "__main__":
    main()
