# Hedgineer Take-Home

### Setup

This is just to get up and running, to see my actual write-up on my solution and my please go [here](#development)

Requires:

- `Python 3.12+`

In the project directory (assumes a Unix-style environment, substitute with Windows specific commands as required):

```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Running the Interactive Builder

Running `python -m hedgineer` will output the Security Master built from the test audit trail

There are various flags that can be used to do more interesting things:

- `-g` or `--generate` will generate random data which can be a more interesting experience
- `-m` or `--merge` will take the Security Master and merge it with a simulated _second_ audit trail
- `-f` or `--filter` takes a string argument and will only results whose `asset_class` matches the argument. Pass in `none` to see uncategorized assets
- `p` or `--positions` will join the Security Master on the mock position data and output the result
- `s` or `--sql` will echo mock SQL commands that would write/read the Security Master to a SQL database

The behaviours will stack, so `python -m hedgineer -g -f equity -p` will generate mock data, filter it by equities, and output the joined positions (potentially none). However, `-g`can't be used with `-m`

Running `pytest` will output test results (which should all pass)

I tried to be mindful that you all may be on Windows, so everything should work in a Windows environment as well, although I haven't been able to test it yet

# Development

Here I document my data transformation and how I arrived at my solution. I've written this portion as I might write a technical blog post or an internal report to colleagues.

## Building a Security Master

Imagine we want to collect data on securities in order to invest and trade.
