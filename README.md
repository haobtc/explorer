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


