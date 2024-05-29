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

## The Quick Version

The quick version if you don't want to read the more elaborate version below:

- Our facts arrive to us in audit stream, a stream of key-value pairs indexed by an id for a security along with an effective date
- We want to convert this into a structured table per (security_id, effective_date), which is essentially a **hierarchical key**
- We can bucket together the key-value pairs ("facts") respecting this hierarchical key, so pairs with the same (security_id, effective_date) are collected together
- We can sort these buckets first by security_id, and then effective_date
- We can now finally assemble a table indexed by this hierarchical key by carefully stiching together buckets with the same security_id, taking note of which facts change in each bucket (i.e. on a new date) and propagating those to future rows in the table (of the same security of course)
- Once we have this table we can join it on other tables like position data which gives us an overview of our positions over time along with the characteristics of their component securities
- We can also go deeper in 3 ways:
- **A** We want to selectively view different tables for different types of assets, because fixed-income assets have very different properties than equity assets. I solved this in a very straightforward way after almost completely over-complicating it
- **B** The world continues to turn. As we accrue new facts of information, we might not want to recompute the Security Master from scratch. We can bucket batches of facts that arrive into an update (a batch of new facts) and selectively _merge_ this new batch of information into the existing table. There are some nuance and edge cases here, i.e. handling new columns in the table due to new key-value pairs
- **C** We might want to easily export our security master to other tools for analysis or persistence. I show how we might do that using Apache Arrow

## Building a Security Master

Imagine we want to collect data on securities in order to invest and trade. We can assume that we can collect "facts" about these securities from various sources - our task is to collate this into a single reliable source-of-truth to use downstream. In essence we want a single "master" table that we can query as required, which contains every available fact at each in point in time.

There are a couple of interesting challenges to note here:

1. We can't really say in advance what kind of facts we'll collect about securities. Although we might be able to enumerate every single property we're interested in _now_, the world is a moving target, and our needs might change. Additionally, we can't rely on data vendors not to change their methods or what information they will be able to provide. Conclusion: we can't really enforce a strict schema

2. We don't have complete control over when facts arrive to us. Companies delay announcements, providers have outages, errors are made etc. We need to be able to react to data as it comes in

3. We want to maintain a _history_ of facts and changes, not solely the most current data. So we need to maintain a snapshot of securities at _various points in time_

### Audit Trail

Let's collect these facts into an audit trail that looks like the following:

```python
AUDIT_TRAIL: list[AuditFact] = [
    (1, "asset_class", "equity", parse_date("01/01/24")),
    (1, "ticker", "GRPH", parse_date("01/01/24")),
    (1, "name", "Graphite bio", parse_date("01/01/24")),
    (1, "ticker", "LENZ", parse_date("03/22/24")),
    (1, "name", "Lenz Therapeutics, Inc", parse_date("03/22/24")),
    ...
]
```

where the first column represents the `security_id`, the second and third the key and value attribute pair, respectively, and the fourth the `effective_date` of the fact. Here `parse_date` simply converts the textual date into a `datetime.date` object.

We can think of our Security Master as being indexed by a hierarchical key, i.e. (security_id, effective_date).

### Bucketing Facts

Our first task will be to collect these into buckets for each `security_id` and `effective_date`. Using a relatively obscure Python API (`dict.setdefault()`), we can easily do this using `reduce`:

```python
def bucket_fact(bucket: dict[int, dict[date, list[AttributePair]]], fact: AuditFact):
    security_id, attribute_key, value, effective_date = fact
    bucket.setdefault(security_id, {effective_date: []}).setdefault(
        effective_date, []
    ).append((attribute_key, value))

    return bucket

def bucket_facts(audit_trail: AuditTrail) -> dict[int, dict[date, list[AttributePair]]]:
    return reduce(bucket_fact, audit_trail, {})
```

Once we have our buckets, we want to flatten them out, or more specifically, given a nested dict of the form: `{1: {date(2024, 1, 1): [("name", "abc"), ("ticker", "ABC")]}}` we want to generate tuples of the form: `(1, date(2024, 1, 1), [("name", "abc"), ("ticker", "ABC")])`. I came up with the following general (i.e. arbitrarily nested) way of doing this in Python - there are likely other ways that are just as good:

```python
def deeply_spread(dd: dict[Any, Any]):
    result: list = []

    for k, v in dd.items():
        if isinstance(v, dict):
            result.extend(map(lambda k_: (k, *k_), deeply_spread(v)))
        else:
            result.append((k, v))

    return result
```

Once we've done that it's relatively straightforward to sort these spread tuples:

```python
sorted(deeply_spread(bucketed_facts), key=itemgetter(0, 1))
```

### The Table Header

We're not ready to build the table just yet. We first need to extract the column names (i.e. the header) as well as their _order_. The following code collects all of the keys present in the attribute pairs from the audit trail as well as adds three column headers that we don't expect in these keys (the `security_id` and the date range). We can sort the header based upon a globally-defined priority measure, and alphabetically thereafter. We also need a reverse index mapping a column header value back to its indexed position in the header. This will be very important for doing transformations when we construct and merge the table.

```python
ATTRIBUTE_PRIORITY = {
    "security_id": 0,
    "effective_start_date": 1,
    "effective_end_date": 2,
    "asset_class": 3,
    "ticker": 4,
    "name": 5,
}


def extract_header(
    audit_trail: AuditTrail, attribute_priority: dict[str, int]
) -> tuple[Header, ColumnIndex]:
    header = [
        "security_id",
        "effective_start_date",
        "effective_end_date",
        *list(dict.fromkeys(map(lambda raw_fact: raw_fact[1], audit_trail)).keys()),
    ]
    header = sorted(header, key=lambda x: (attribute_priority.get(x, float("inf")), x))
    col_index = {v: i for i, v in enumerate(header)}

    return header, col_index
```

By modifying the `ATTRIBUTE_PRIORITY` variables, we can change the order of the columns in the printed tables, if we wanted to.

### Building the Table

We now have all of the requisite parts to build our table:

- The header values
- The sorted flat facts, which as a reminder are of the form `(security_id, date, [*kv_pairs])`
- The column index
  An important point that we can take advantage of when constructing our table is that the flat, bucketed facts from before are **_already sorted in the table order we require_**. This is hugely beneficial because it means we can iterate over our buckets and build rows in the table solely by examining the available facts in the bucket as well as the information in the prior row of the same security (if avaiable).

Here I'll start at the high-level, there isn't much to see yet:

```python
def generate_data_from_facts(
    sorted_flat_facts: list[FlatFactSet],
    col_index: ColumnIndex,
) -> SMData:
    sm_data, _ = reduce(accumulate_fact, sorted_flat_facts, ([], col_index))
    return sm_data
```

Not much going on yet, we're reducing over our `sorted_flat_facts` into a list via the `accumulate_fact` function. Let's examine that:

```python
def accumulate_fact(
    data__col_index: tuple[SMData, ColumnIndex], flat_fact: FlatFactSet
):
    data, col_index = data__col_index
    security_id, effective_date, _ = flat_fact

    is_new_security = (
        len(data) == 0 or data[-1][col_index["security_id"]] != security_id
    )
    prior_row = generate_none_tuple(len(col_index)) if is_new_security else data[-1]
    new_row = diff_row(prior_row, col_index, flat_fact)

    # Modify the last row's end date
    if not is_new_security:
        index = col_index["effective_end_date"]
        data[-1] = tuple((*data[-1][:index], effective_date, *data[-1][index + 1 :]))

    # Now we can append the new row
    data.append(new_row)
    return (data, col_index)
```

Let's unpack this. We first destructure the accumulator variable into the actual list that represents our accumulated table data as well as the column index we generated before. We also extract the the `security_id` and `effective_date` from the flat fact tuple.

We then check if the row we are inserting into the table represents a new security. This happens for the first row in the table or when the prior row has a different `security_id`. If we are inserting a row for a new security, our facts will represent the first pieces of data in time for this security (because our facts are very helpfully sorted). In this case, we use a helper `generate_none_tuple` to generate a tuple with the appopriate amount of `None` values. We'll use this to diff with our facts in just a moment.

In the case that there is already a row in the table for the security in question, we want to instead copy this previous row, as our new facts will instead represent diffs to this **prior information**. There is a nice unity to the API here in both of these cases, in that once we have the prior row regardless if it's from the same security or a new one, we can call `diff_row` with our facts to get the tuple correctly updated with the information from our facts. We can now also see the importance of the column index used to index into our prior row. If the row is for a new security, we generate a tuple filled with the appropriate number of `None` values, otherwise we copy the prior row.
