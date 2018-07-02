__version__ = '0.0.0'

import os
import datetime


class FailSampler(object):
    def __init__(self, save_dir=os.path.join(os.getcwd(), 'fail_samples'), save_filename=None, sample_method='random', sample_size=None, random_state=None):
        sample_method = sample_method.lower()
        if sample_method not in ['random', 'head', 'tail']:
            raise ValueError("`sample_method` must be one of 'random', 'head', or 'tail'")
        self.sample_size = sample_size
        self.sample_method = sample_method
        self.random_state = random_state
        self.save_dir = save_dir
        self.save_filename = save_filename
    
    def take_sample(self, df, fails_index):
        fails = df[fails_index]
        if not self.sample_size:
            return fails
        elif self.sample_method=='random':
            return fails.sample(self.sample_size, random_state=self.random_state)
        elif self.sample_method=='tail':
            return fails.tail(self.sample_size)
        else:
            return fails.head(self.sample_size)
    
    def write_sample(self, df, fails_index, save_dir=None, save_filename=None, *args, **kwargs):
        save_dir = save_dir or self.save_dir
        save_filename = save_filename or self.save_filename or 'FailSample {dt}'.format(dt=datetime.datetime.now().strftime("%Y-%m-%d-%H%M%s"))
        save_path = os.path.join(save_dir, save_filename)
        self.take_sample(df, fails_index).to_csv(save_path, *args, **kwargs)


class DataFrameInspector(object):
    def __init__(self, title='DataFrame Check', detail=None, logger=None, fail_sampler=None):
        self.title = title
        self.detail = detail
        self.logger = logger
        self.fail_sampler = fail_sampler
    
    def make_log_msg(self, status, extra=None, sep='::'):
        """
        Make a message that looks like this:
        Title::status::detail::extra
        Any None will be skipped.
        """
        strings = [self.title, status, self.detail, extra]
        return sep.join(filter(None, strings))
    
    def inspect(self, df):
        pass

        

class QARoutine(object):
    pass