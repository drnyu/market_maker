# kalshi_market_maker
To set up the archival tool, you need to create a Kalshi Account and go to Settings to create an API key. This might require KYC verification where you need a government ID.

After that, create a .env file in the main directory and a crt.pem file.

In the env file put 
```
KALSHI_API_KEY={KALSHI-KEY}
KALSHI_URL=https://api.elections.kalshi.com/trade-api/v2
PROD_KEYFILE=crt.pem
```

In the crt.pem file put the RSA Key. This will look like:
```
-----BEGIN RSA PRIVATE KEY-----
{RSA KEY CONTENT}
-----END RSA PRIVATE KEY-----
```

Once this is complete make sure you have all required packages (should be in requirements.txt) and that the pathing for 
```
from clients import Environment
from KalshiClient import ExchangeClient
```
is correct so that you can use the files that have the API calls.
