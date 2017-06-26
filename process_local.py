import os
import subprocess
import glob

import click


class JobLogBook(object):
    def __init__(self, n_jobs=1, log_dir=None):
        self.log_dir = log_dir
        self.logbook = {}
        self.n_jobs = n_jobs
        self.n_running = 0
        self.n_finished = 0

    def process(self, binaries):
        with click.progressbar(length=len(binaries)) as bar:
            for job in binaries:
                if os.path.isfile(job) and os.access(job, os.X_OK):
                    self.__start_subprocess__(job)
                else:
                    click.echo('{} is not executable! (Skipped)')
                    self.n_finished += 1
                if self.n_running >= self.n_jobs:
                    self.__wait__(bar)
            click.echo('All Jobs started. Wait for last jobs to finish!')
            while self.n_running > 0:
                self.__wait__(bar)
            bar.update(self.n_finished)
        click.echo('Finished!')

    def __wait__(self, progressbar):
        pid, exit_code = os.wait()
        job_file = self.logbook[pid][1]
        click.echo('{} finished with exit code {}'.format(
            job_file, exit_code))
        self.n_finished += 1
        self.__clear_job__(pid)
        progressbar.update(self.n_finished)

    def __start_subprocess__(self, job):
        job_name = os.path.splitext(job)[0]
        if self.log_dir is not None:
            log_path = os.path.join(self.log_dir, '{}.log'.format(job_name))
            log_file = open(log_path, 'w')
        else:
            log_file = open(os.devnull, 'w')
        sub_process = subprocess.Popen([job],
                                       stdout=log_file,
                                       stderr=subprocess.STDOUT)
        self.logbook[sub_process.pid] = [sub_process,
                                         job,
                                         log_file]
        self.n_started += 1
        return sub_process.pid

    def __clear_job__(self, pid):
        sub_process, job, log_file = self.logbook[pid]
        self.n_running -= 1
        if self.log_dir is not None:
            log_file.close()
        del self.logbook[pid]


@click.command()
@click.argument('path', click.Path(exists=True))
@click.option('-j', '--n_jobs', default=1,
              help='Number of parallel jobs')
@click.option('-p', '--binary_pattern', default='*.sh',
              help='Pattern of the binaries')
@click.option('-l', '--log_path', default=None,
              help='Path to a dir where the stdout/stderr should be saved')
def main(path, binary_pattern, n_jobs, log_path):
    binaries = list(glob.glob(os.path.join(path, binary_pattern)))
    click.echo('Processing {} with max. {} parralel jobs!'.format(
        len(binaries), n_jobs))
    click.echo('Starting processing!')
    log_book = JobLogBook(n_jobs=n_jobs, log_dir=log_path)
    log_book.process(binaries)

if __name__ == '__main__':
    main()
