import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional


_FLAGS = set()


@dataclass
class TaskConfig:
    id: str
    type: str
    inputs: Dict[str, str] = field(default_factory=dict)
    outputs: Dict[str, str] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    # dependencies: List[str] = None  # IDs de tarefas dependentes
    

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

    @abstractmethod
    def process(self) -> bool:
        """Executa o processamento principal"""
        pass
