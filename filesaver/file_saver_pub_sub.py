import os
import json
from time import sleep
from datetime import datetime
from typing import Any, Dict, Tuple

import schedule
import paho.mqtt.client as mqtt

from base_mqtt_pub_sub import BaseMQTTPubSub


class FileSaverPubSub(BaseMQTTPubSub):
    def __init__(
        self: Any,
        sensor_save_topic: str,
        telemetry_save_topic: str,
        c2c_topic: str,
        data_root: str,
        sensor_directory_name: str,
        telemetry_directory_name: str,
        sensor_file_prefix: str,
        telemetry_file_prefix: str,
        **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)

        self.sensor_save_topic = sensor_save_topic
        self.telemetry_save_topic = telemetry_save_topic
        self.c2c_topic = c2c_topic

        self.sensor_save_path = os.path.join(data_root, sensor_directory_name)
        self.telemetry_save_path = os.path.join(data_root, telemetry_directory_name)

        self.sensor_file_prefix = sensor_file_prefix
        self.telemetry_file_prefix = telemetry_file_prefix

        self.sensor_file_timestamp = ""
        self.telemetry_file_timestamp = ""

        self.file_suffix = ".json"

        self.sensor_file_name = (
            self.sensor_file_prefix + self.sensor_file_timestamp + self.file_suffix
        )
        self.telemetry_file_name = (
            self.telemetry_file_prefix
            + self.telemetry_file_timestamp
            + self.file_suffix
        )

        self.prev_sensor_file = None
        self.prev_telemetry_file = None

        self.connect_client()
        sleep(1)
        self.publish_registration("File Saver Registration")

        os.makedirs(self.sensor_save_path, exist_ok=True)
        os.makedirs(self.telemetry_save_path, exist_ok=True)

        self.sensor_file_path = None
        self.telemetry_file_path = None

        self.prev_sensor_file, self.sensor_file_path = self._setup_new_write_file(
            self.sensor_file_prefix,
            self.sensor_save_path,
            self.prev_sensor_file,
            self.sensor_file_path,
        )
        self.prev_telemetry_file, self.telemetry_file_path = self._setup_new_write_file(
            self.telemetry_file_prefix,
            self.telemetry_save_path,
            self.prev_telemetry_file,
            self.telemetry_file_path,
        )

    def _setup_new_write_file(
        self: Any, file_prefix: str, save_path: str, prev_file: str, file_path: str
    ) -> Tuple:
        file_timestamp = str(int(datetime.utcnow().timestamp()))
        file_name = file_prefix + file_timestamp + self.file_suffix
        file_path = os.path.join(save_path, file_name)

        with open(file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("[")

        if not prev_file:
            prev_file = file_path
        else:
            with open(prev_file, encoding="utf-8", mode="a") as file_pointer:
                file_pointer.write("\n]")
            prev_file = file_path
        return prev_file, file_path

    def _sensor_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        payload_json_str = str(msg.payload.decode("utf-8"))
        with open(self.sensor_file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _telemetry_save_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        payload_json_str = str(msg.payload.decode("utf-8"))
        with open(self.telemetry_file_path, encoding="utf-8", mode="a") as file_pointer:
            file_pointer.write("\n\t" + payload_json_str + ",")

    def _c2c_callback(
        self: Any, _client: mqtt.Client, _userdata: Dict[Any, Any], msg: Any
    ) -> None:
        c2c_payload = json.loads(str(msg.payload.decode("utf-8")))
        if c2c_payload["msg"] == "NEW FILE":
            self.prev_sensor_file, self.sensor_file_path = self._setup_new_write_file(
                self.sensor_file_prefix,
                self.sensor_save_path,
                self.prev_sensor_file,
                self.sensor_file_path,
            )
            (
                self.prev_telemetry_file,
                self.telemetry_file_path,
            ) = self._setup_new_write_file(
                self.telemetry_file_prefix,
                self.telemetry_save_path,
                self.prev_telemetry_file,
                self.telemetry_file_path,
            )

    def main(self: Any) -> None:
        schedule.every(10).seconds.do(
            self.publish_heartbeat, payload="File Saver Heartbeat"
        )

        self.add_subscribe_topics(
            [
                self.sensor_save_topic,
                self.telemetry_save_topic,
                self.c2c_topic,
            ],
            [
                self._sensor_save_callback,
                self._telemetry_save_callback,
                self._c2c_callback,
            ],
            [2, 2, 2],
        )

        while True:
            try:
                schedule.run_pending()
                sleep(0.001)
            except Exception as e:
                print(e)

                if self.sensor_file_path:
                    with open(
                        self.sensor_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")

                if self.telemetry_file_path:
                    with open(
                        self.telemetry_file_path, encoding="utf-8", mode="a"
                    ) as file_pointer:
                        file_pointer.write("\n]")


if __name__ == "__main__":
    saver = FileSaverPubSub(
        sensor_save_topic=os.environ.get("SENSOR_SAVE_TOPIC"),
        telemetry_save_topic=os.environ.get("TELEMETRY_SAVE_TOPIC"),
        c2c_topic=os.environ.get("C2_TOPIC"),
        data_root=os.environ.get("DATA_ROOT"),
        sensor_directory_name=os.environ.get("SENSOR_DIR"),
        telemetry_directory_name=os.environ.get("TELEMETRY_DIR"),
        sensor_file_prefix=os.environ.get("SENSOR_FILE_PREFIX"),
        telemetry_file_prefix=os.environ.get("TELEMETRY_FILE_PREFIX"),
        mqtt_ip=os.environ.get("MQTT_IP"),
    )
    saver.main()
