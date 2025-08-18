import argparse
import importlib
import networkx as nx
import sys
import time
import threading
import yaml
from pprint import pprint
from pathlib import Path

from core.task import BaseTask, TaskConfig


_ERROR_CODE_NO_CONFIG = 1
_ERROR_CODE_CONFIG_DUPLICATE_ID = 2
_ERROR_CODE_CONFIG_NO_TASK = 3
_ERROR_CODE_TASK_ERROR = 4


class Main:
    """
    Classe principal do programa
    """

    def __init__(self):
        self._BASE_DIR = sys.path[0]
        self._BASE_PATH = Path(self._BASE_DIR)
        self.arg_parser()
        self.process_config()

    def arg_parser(self):
        """Define argumentos da linha de comando"""
        parser = argparse.ArgumentParser(description='Pipeline de Processamento Geoespacial')
        parser.add_argument('--config', default='config/pipeline_config.yaml',
                        help='Arquivo de configuração do pipeline')
        parser.add_argument('--print-config', action='store_true',
                            help='Imprime configuração do pipeline')
        parser.add_argument('--task-id', help='ID da tarefa (modo task)')
        parser.add_argument('--orquestrador', action='store_true',
                            help='Executa o orquestrador (modo orquestrador)')

        self.args = parser.parse_args()

    def process_config(self):
        self.load_config()
        self.validate_config()
        self.tasks_config()
        self.grafo_config()

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

    def tasks_config(self):
        self.tasks = []
        for task in self.config['tasks']:
            module_name = f"tasks.{task['type']}"
            module = importlib.import_module(module_name)
            class_name_words = task['type'].split('_') + ['Task']
            class_name = ''.join(word.capitalize() for word in class_name_words)
            task_config = TaskConfig(
                id=task['id'],
                type=task['type'],
                inputs=task.get('inputs'),
                outputs=task.get('outputs'),
                parameters=task.get('parameters'),
            )
            TaskClass : BaseTask = getattr(module, class_name)
            task = TaskClass(task_config)
            self.tasks.append(task)

    def grafo_config(self):
        self.grafo = nx.DiGraph()
        for task in self.config['tasks']:
            self.grafo.add_node(task['id'], task=task)
            for input in task.get('inputs', {}).items():
                for prox_task in self.config['tasks']:
                    if input in prox_task.get('outputs', {}).items():
                        self.grafo.add_edge(prox_task['id'], task['id'])

    def print_config(self):
        pprint(self.config)

    def run_single_task(self, task_id: str):
        task = self.get_task(task_id)
        if not task:
            print(f"Task {task_id} not found")
            sys.exit(_ERROR_CODE_CONFIG_NO_TASK)
        success = task.process()
        sys.exit(0 if success else _ERROR_CODE_TASK_ERROR)

    def get_task(self, task_id: str):
        for task in self.tasks:
            if task.config.id == task_id:
                return task
        return None

    def orquestrador(self, grafo, timeout=10):
        tarefas_executadas = set()
        total_sleep = 0
        while len(tarefas_executadas) < len(grafo.nodes) and total_sleep < timeout:
            for node in grafo.nodes:
                tarefa = grafo.nodes[node]['task']
                predecessores = list(grafo.predecessors(node))
                if all(pred in tarefas_executadas for pred in predecessores):
                    if tarefa.is_ready() and node not in tarefas_executadas:
                        total_sleep = 0
                        thread = threading.Thread(target=tarefa.run)
                        thread.start()
                        tarefas_executadas.add(node)
            time.sleep(1)
            total_sleep += 1

    def main(self):
        if self.args.print_config:
            self.print_config()
            return

        if self.args.task_id:
            self.run_single_task(self.args.task_id)
            return
        
        if self.args.orquestrador:
            self.orquestrador(self.grafo)


if __name__ == "__main__":
    Main().main()
