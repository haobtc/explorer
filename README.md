explorer
==========
Blockchain for bitcoin and altcoins

install
==========

```
% cd <path/to/explorer>
% git submodule update --init --recursive
% pushd alliances/bitcore
% npm install
% popd
% npm install
% cp config.template.json config.json
```

Run
=======

Start a node server
% node start.js -s node

Start a node server at different port and only for dogecoin and bitcoin
% node start.js -s node -c dogecoin -c bitcoin -p 8335

For query server 
% node start.js


