from core.task import BaseTask


class PrinterTask(BaseTask):

    def _process(self):
        print(self.config.parameters['text'])
        return True
