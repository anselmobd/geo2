import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any


_FLAGS = set()


@dataclass
class TaskConfig:
    id: str
    type: str
    inputs: Dict[str, str] = None
    outputs: Dict[str, str] = None
    parameters: Dict[str, Any] = None
    # dependencies: List[str] = None  # IDs de tarefas dependentes
    
    def __post_init__(self):
        if self.inputs is None:
            self.inputs = {}
        if self.outputs is None:
            self.outputs = {}
        if self.parameters is None:
            self.parameters = {}

class BaseTask(ABC):
    def __init__(self, config:TaskConfig):
        self.config = config

    def input_ready(self, key, value):
        if key == 'file':
            return os.path.exists(value)
        elif key == 'flag':
            return value in _FLAGS 
        return False

    def is_ready(self):
        if not self.config.inputs:
            return True
        return all(self.input_ready(key, value) for key, value in self.config.inputs.items())

    def process(self) -> bool:
        result = self._process()
        if result:
            flag = self.config.outputs.get('flag')
            if flag:
                _FLAGS.add(flag)   
        return result

    @abstractmethod
    def _process(self) -> bool:
        """Executa o processamento principal"""
        pass
