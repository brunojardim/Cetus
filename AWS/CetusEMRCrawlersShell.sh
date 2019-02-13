#!/bin/bash -xe
echo "--------------------------- ENTROU na SHELL -----------------------------------"
#set -x

echo "Entrando na pasta Cetus"
cd /home/hadoop/cetus

USERS=(bruno bruno_jaja felipe fernanda_lisboa frazzato juliao fernanda_scarpelli rauhe renato_jones rodrigo tamara)
# TOKEN=$(sudo docker exec jupyterhub /opt/conda/bin/jupyterhub token jovyan | tail -1)
for i in "${USERS[@]}"; 
do 
   sudo docker exec jupyterhub useradd -m -s /bin/bash -N $i
   sudo docker exec jupyterhub bash -c "echo $i:cetus@jupyter | chpasswd"
done

sudo docker exec jupyterhub pip install bs4