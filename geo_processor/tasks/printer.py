from core.task import BaseTask, _FLAGS


class PrinterTask(BaseTask):

    def _process(self):
        print(self.config.parameters['text'])
        return True
