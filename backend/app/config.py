from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    gcp_project_id: str = "project-b2f8b5ee-abac-4b3f-940"
    pubsub_subscription: str = "vh-details"
    google_maps_api_key: str = ""
    digitransit_api_key: str = ""

    vehicle_timeout_seconds: int = 300
    stats_broadcast_interval: int = 1
    vehicle_flush_interval: float = 0.1

    pubsub_worker_count: int = 4
    pubsub_queue_maxsize: int = 20000
    pubsub_flow_max_messages: int = 1000
    pubsub_flow_max_bytes: int = 20 * 1024 * 1024

    @property
    def subscription_path(self) -> str:
        return f"projects/{self.gcp_project_id}/subscriptions/{self.pubsub_subscription}"


settings = Settings()
