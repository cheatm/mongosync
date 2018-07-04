FROM daocloud.io/xingetouzi/python3-cron
COPY requirements.txt ./

RUN pip install --no-cache-dir -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

COPY . ./

RUN python setup.py install
RUN crontab $PWD/routing/timelist

CMD ["/usr/sbin/cron", "-f"]