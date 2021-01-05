(examples)=

# Examples

## Creating and running basis Process

A Plumpy process can be create and run by:

1. Copy and paste the following code block into a new file called ``helloWorld.py``:

   ```{literalinclude} ../../../examples/process_helloworld.py
   ```

2. run the process:

   ```console
   $ python helloWorld.py
   ```

## Process can wait, pause, play and resume

The example below shows how process state transition with different action:

```{literalinclude} ../../../examples/process_wait_and_resume.py
```

<!-- ## Remote controlled process

process start.

script to kill that process -->

## Creating and running basic WorkChain

The WorkChain is a special process that can strung different small function
together into a independent process.

See the example below:

```{literalinclude} ../../../examples/workchain_simple.py
```
