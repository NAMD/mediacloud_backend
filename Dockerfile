# mediacloud_backend
#
# VERSION               0.1.0

FROM                    ubuntu
MAINTAINER              Álvaro Justen <alvarojusten@gmail.com>

# update the package list
RUN echo 'deb http://archive.ubuntu.com/ubuntu precise main universe' > /etc/apt/sources.list
RUN apt-get update

# install system dependencies and clean APT cache
RUN apt-get -y install python-dev python-pip wget build-essential libxml2-dev libxslt1-dev zlib1g-dev libssl-dev mongodb
RUN apt-get clean

# download, compile and install httrack
RUN wget -O /tmp/httrack.tar.gz http://download.httrack.com/cserv.php3?File=httrack.tar.gz
RUN cd /tmp; tar xfz httrack.tar.gz
RUN cd /tmp/httrack-*; ./configure && make && make install
RUN ln -s /usr/local/lib/libhttrack.* /usr/lib/
RUN rm -rf /tmp/httrack*

# install Python dependencies from PyPI using pip
ADD ./capture/ /srv/mediacloud_backend/capture/
ADD ./requirements.txt /srv/mediacloud_backend/
RUN cd /srv/mediacloud_backend/; pip install -r requirements.txt
