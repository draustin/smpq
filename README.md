# smpq
Simple Multi-Processing with Queues

## Introduction
My first serious attempt at multiprocessing with Python ended in failure, in that
there was no significant speedup. I used multiprocessing.Pool to parallelize
the calculation of HHG spectra in benzene over different molecular angles. The
overhead of setting up the process each time (several seconds, worse on Windows)
prevented any improvement.

Following [https://pymotw.com/2/multiprocessing/communication.html]  I wrote this simple system based on multiprocessing.Queue. The processes are created once, and tasks passed from the main process to the worker processes using
the queue.

## Features
- Worker 'retirement' after a specified number of tasks - useful for e.g. dealing with slow memory leaks.
- Detailed, configurable logging.
- Memory efficiency using generators.
