from core.task import BaseTask


class PrinterTask(BaseTask):

    def process(self):
        print(self.config.parameters['text'])
