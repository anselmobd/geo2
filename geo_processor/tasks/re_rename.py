import glob
import os
import re

from core.task import BaseTask


class ReRenameTask(BaseTask):

    def _process(self):
        files = glob.glob(self.config.inputs['file'])
        for file in files:
            new_name = re.sub(self.config.parameters['match'], self.config.outputs['file'], file)
            print(f"Renaming {file} to {new_name}")
            # os.rename(file, new_name)
        return True
