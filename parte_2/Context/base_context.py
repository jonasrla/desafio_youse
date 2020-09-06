from abc import ABC, abstractmethod

class BaseContext(ABC):
    def transformation(self, data):
        return data
    def save(self, data):
        return 'success'
