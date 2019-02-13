#!/bin/bash -xe




#Criando pasta para as funcoes do Turing
sudo mkdir -p /home/hadoop/cetus

# copiando funcoes do S3 para o hadoop
cd /home/hadoop/cetus
sudo aws s3 cp s3://cetus-codigos/shells/CetusEMRCrawlersShell.sh .

sudo aws s3 cp s3://cetus-codigos/shells/CetusEMRShell_p1.sh .





