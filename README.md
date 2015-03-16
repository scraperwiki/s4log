# s4log simply pushes logs to S3

It works by invoking a command and reading its stdout into a buffer.

When the buffer is full, s4log asynchronously pushes that buffer into S3
with the hostname and current timestamp.

In addition, s4log will also push logs on a regular basis if there are not
enough logs to fill the buffer.

This is not intended to replace other types of logging infrastructure, just
a way to easily and reliably store logs in S3 with minimal configuration.

Authentication to the bucket is best done through IAM roles, so you should
make an S3 bucket and add a policy which allows the machine to put files into
it.

This is an initial proof of concept release. Feel free to play around and
feedback is welcomed.