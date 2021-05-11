# Copyright (c) 2002 Toby Dickenson and contributors
#
# This library is subject to the provisions of the
# GNU Lesser General Public License version 2.1

import os, sys, traceback, time

class pipeline:
    def __init__(self):
        self.input = None
        self.output = None
        self.winput = None
        self.routput = None

    def set_input(self,input):
        """ specify the file descriptor that the first process will read input from
        """
        self.input = os.dup(input)

    def set_output(self,output):
        """ specify the file descriptor that the last process will write output to
        """
        self.output = os.dup(output)

    def pipe_input(self):
        """ specify that the first process reads frome a pipe. It returns
        a file descriptor that can be written to
        """
        r,w = os.pipe()
        self.input = r
        self.winput = w
        return w

    def pipe_output(self):
        """ specify that the last process writes to a pipe. It returns
        a file descriptor that can be read from
        """
        r,w = os.pipe()
        self.output = w
        self.routput = r
        return r

    def run(self,*processes):
        """ 'processes' is a list of callable objects. It calls each one in a forked
        process, with stdout of one piped to stdin of the next.

        The callable objects should internally call os.exec, or os._exit.
        A normal return will be converted to exit code 0, and an exception
        converted to exit code 1.

        Call this only once.
        """
        if len(processes)<1:
            raise ValueError('A pipeline needs at least 1 process')
        if self.input is None:
            self.input = os.open('/dev/null',os.O_RDONLY)
        if self.output is None:
            self.output = os.open('/dev/null',os.O_WRONLY)
        # probably always safe
        sys.stdout.flush()
        # create the pipes
        outputs = []
        inputs = [self.input]
        for p in processes[:-1]:
            r,w = os.pipe()
            outputs.append(w)
            inputs.append(r)
        outputs.append(self.output)
        # create the processes
        self.pids = []
        for input,process,output in zip(inputs,processes,outputs):
            pid = os.fork()
            if pid:
                # Parent
                self.pids.append(pid)
            else:
                # Child
                try:
                    # Replace stdin
                    if input!=0:
                        os.close(0)
                        os.dup(input)
                    # Replace stdout
                    if output!=1:
                        os.close(1)
                        os.dup(output)
                    # Close the pipes
                    for fd in inputs:
                        os.close(fd)
                    for fd in outputs:
                        os.close(fd)
                    if self.routput:
                        os.close(self.routput)
                    if self.winput:
                        os.close(self.winput)
                    process()
                except:
                    traceback.print_exc()
                    os._exit(1)
                else:
                    os._exit(0)
        # Close the pipes in the parent
        for fd in inputs:
            os.close(fd)
        for fd in outputs:
            os.close(fd)

    def close(self):
        """ Wait for the processes to exit and return a list of process exit codes.
        Those codes are later available in the .codes attribute
        """
        # Close the pipes
        self.codes = []
        for pid in self.pids:
            junk,status = os.waitpid(pid,0)
            self.codes.append(status)

    def all_ok(self):
        """ Return true iff every process exited zero
        """
        for code in self.codes:
            if code:
                return 0
        return 1

def test():
    p = pipeline()
    w = os.fdopen(p.pipe_input(),'w')
    r = os.fdopen(p.pipe_output(),'r')
    p.run(test_cap,test_sed,test_cap,test_sed,test_cap)
    w.write('Hello world\na\n')
    w.close()
    assert r.read()=='HELLO WORLD\nAAA\n'
    p.close()
    assert p.all_ok()

    p = pipeline()
    r = os.fdopen(p.pipe_output(),'r')
    p.run(test_gen,test_cap)
    assert r.read()=='ABC'
    p.close()
    assert p.all_ok()

    print 'ok'

def test_cap():
    s = sys.stdin.read()
    sys.stdout.write(s.upper())
    sys.stdout.flush()
    sys.stdout.close()

def test_sed():
    os.execlp('sed',  'sed', '-e', 's/A/aa/')

def test_gen():
    os.write(1,'abc')
            
if __name__=='__main__':
    test()



