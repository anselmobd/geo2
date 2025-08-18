import argparse
import sys
import yaml
from pprint import pprint


_ERROR_CODE_NO_CONFIG = 1


class Main:
    """
    Classe principal do programa
    """

    def __init__(self):
        self.arg_parser()
        self.load_config()

    def arg_parser(self):
        """Define argumentos da linha de comando"""
        parser = argparse.ArgumentParser(description='Pipeline de Processamento Geoespacial')
        parser.add_argument('--config', default='config/pipeline_config.yaml',
                        help='Arquivo de configuração do pipeline')
        parser.add_argument('--print-config', action='store_true',
                            help='Imprime configuração do pipeline')
        parser.add_argument('--task-id', help='ID da tarefa (modo task)')

        self.args = parser.parse_args()

    def load_config(self):
        """
        Carrega configuração do pipeline para a memória
        """
        try:
            with open(self.args.config, 'r') as f:
                self.config = yaml.safe_load(f)
            print(f"Config file: {self.args.config}")
        except FileNotFoundError:
            print("No config file provided")
            sys.exit(_ERROR_CODE_NO_CONFIG)

    def print_config(self):
        pprint(self.config)

    def run_single_task(self, task_id: str):
        if self.task_exists(task_id):
            print(f"Running geo2 task: {task_id}")
        else:
            print(f"Task {task_id} not found")

    def task_exists(self, task_id: str):
        return any(task['id'] == task_id for task in self.config['tasks'])

    def main(self):
        if self.args.print_config:
            self.print_config()
            return

        if self.args.task_id:
            self.run_single_task(self.args.task_id)


if __name__ == "__main__":
    Main().main()
