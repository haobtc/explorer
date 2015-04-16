explorer
==========
Blockchain for bitcoin and altcoins

Install
==========

```
% cd <path/to/explorer>
% npm install
% cp config.template.json config.json
```

Run servers
=======

Start a node server, node server is a p2p node which synchronize blocks from the coins' blockchain.
```
% node start.js -s node
```

Start a node server at different port and only for dogecoin and bitcoin
```
% node start.js -s node -c dogecoin -c bitcoin -p 8335
```

Start a query server 
```
% node start.js
```

Install Services
===========
```
cd <path/to/explorer>
mkdir -p service/logs
sudo ln -s $PWD/service/bin/watch-service /usr/local/bin/watch-service
sudo ln -s $PWD/service/bin/run-service /usr/local/bin/run-service
sudo ln -s $PWD/service/explorer.node /etc/service/explorer.node
sudo ln -s $PWD/service/explorer.mnode /etc/service/explorer.mnode
sudo ln -s $PWD/service/explorer.query /etc/service/explorer.query
sudo ln -s $PWD/service/explorer.txcursor /etc/service/explorer.txcursor

```
