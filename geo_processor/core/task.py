import os
from abc import ABC, abstractmethod


_FLAGS = set()


class BaseTask(ABC):
    def __init__(self, id, inputs, outputs, params):
        self.id = id
        self.inputs = inputs
        self.outputs = outputs
        self.params = params

    def input_ready(self, key, value):
        if key == 'file':
            return os.path.exists(value)
        elif key == 'flag':
            return value in _FLAGS 
        return False

    def is_ready(self):
        if not self.inputs:
            return True
        return all(self.input_ready(input) for input in self.inputs.items())

    @abstractmethod
    def process(self) -> bool:
        """Executa o processamento principal"""
        pass
