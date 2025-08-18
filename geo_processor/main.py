import argparse
import importlib
import sys
import yaml
from pprint import pprint
from pathlib import Path

from core.task import BaseTask


_ERROR_CODE_NO_CONFIG = 1
_ERROR_CODE_CONFIG_DUPLICATE_ID = 2


class Main:
    """
    Classe principal do programa
    """

    def __init__(self):
        self._BASE_DIR = sys.path[0]
        self._BASE_PATH = Path(self._BASE_DIR)
        self.arg_parser()
        self.get_config()

    def arg_parser(self):
        """Define argumentos da linha de comando"""
        parser = argparse.ArgumentParser(description='Pipeline de Processamento Geoespacial')
        parser.add_argument('--config', default='config/pipeline_config.yaml',
                        help='Arquivo de configuração do pipeline')
        parser.add_argument('--print-config', action='store_true',
                            help='Imprime configuração do pipeline')
        parser.add_argument('--task-id', help='ID da tarefa (modo task)')

        self.args = parser.parse_args()

    def get_config(self):
        self.load_config()
        self.validate_config()

    def load_config(self):
        """
        Carrega configuração do pipeline para a memória
        """
        try:
            with open(self._BASE_PATH / self.args.config, 'r') as f:
                self.config = yaml.safe_load(f)
            print(f"Config file: {self.args.config}")
        except FileNotFoundError:
            print("No config file provided")
            sys.exit(_ERROR_CODE_NO_CONFIG)

    def validate_config(self):
        ids = set()
        for task in self.config['tasks']:
            id = task['id']
            if id in ids:
                print(f"Duplicate task id: {id}")
                sys.exit(_ERROR_CODE_CONFIG_DUPLICATE_ID)
            ids.add(id)

    def print_config(self):
        pprint(self.config)

    def run_single_task(self, task_id: str):
        task = self.get_task(task_id)
        if not task:
            print(f"Task {task_id} not found")

        module_name = f"tasks.{task['type']}"
        module = importlib.import_module(module_name)
        
        class_name_words = task['type'].split('_') + ['Task']
        class_name = ''.join(word.capitalize() for word in class_name_words)
        TaskClass = getattr(module, class_name)

        task : BaseTask = TaskClass(
            task['id'],
            task.get('inputs', {}),
            task.get('outputs', {}),
            task['parameters'],
        )
        success = task.process()

        sys.exit(0 if success else 1)

    def get_task(self, task_id: str):
        for task in self.config['tasks']:
            if task['id'] == task_id:
                return task
        return None

    def main(self):
        if self.args.print_config:
            self.print_config()
            return

        if self.args.task_id:
            self.run_single_task(self.args.task_id)


if __name__ == "__main__":
    Main().main()
