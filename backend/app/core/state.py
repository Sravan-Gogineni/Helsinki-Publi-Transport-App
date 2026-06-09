import time
from dataclasses import dataclass, field


@dataclass
class AppState:
    vehicle_state: dict = field(default_factory=dict)
    last_seen: dict = field(default_factory=dict)
    message_timestamps: list = field(default_factory=list)
    pending_updates: dict = field(default_factory=dict)

    def active_vehicles(self, timeout_seconds: int) -> dict:
        cutoff = time.time() - timeout_seconds
        return {
            vid: v
            for vid, v in self.vehicle_state.items()
            if self.last_seen.get(vid, 0) > cutoff
        }


# Module-level singletons shared across the application
app_state = AppState()
