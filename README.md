hadoop-summit-storm-demo
========================

Allen Day's Hadoop Summit Twitter Storm demo

Preparation
-----------

Make sure R is installed on your system and it's accessible from /usr/local/bin/r .

Execute R, and check if the Storm and changepoint libraries are installed by running:

> library('Storm')

> library('changepoint')


If it doesn't, install using the following command. Be aware that you must have write permissions to the default lib dir, so run R process with a user that have them.


> install.packages('Storm')

> install.packages('changepoint')


