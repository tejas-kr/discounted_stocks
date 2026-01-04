from abc import ABC, abstractmethod
import redis

class IRedisClient(ABC):
    @abstractmethod
    def get(self, key: str) -> str | None:
        pass

    @abstractmethod
    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        pass

    @abstractmethod
    def delete(self, key: str) -> int:
        pass

class RedisClient(IRedisClient):
    def __init__(self, url: str):
        self.client = redis.Redis.from_url(url)

    def get(self, key: str) -> str | None:
        return self.client.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        return self.client.set(key, value, ex=ex)

    def exists(self, key: str) -> bool:
        return self.client.exists(key) > 0

    def delete(self, key: str) -> int:
        return self.client.delete(key)