# Hedgineer Take-Home

### Setup

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

I tried to be mindful that you all may be on Windows, so everything should work in a Windows environment as well, although I haven't been able to test it yet, so if that's a problem please let me know

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
AUDIT_TRAIL: list[AuditFace] = [
    (1, "ticker", "LENZ", parse_date("03/22/24")),
    (3, "ticker", "ACME", parse_date("01/01/24")),
    (2, "market_cap", 549000, parse_date("05/23/24")),
    (1, "gics_sector", "healthcare", parse_date("01/01/24")),
    (1, "ticker", "GRPH", parse_date("01/01/24")),
    (1, "name", "Lenz Therapeutics, Inc", parse_date("03/22/24")),
    (2, "ticker", "V", parse_date("01/01/23")),
    (2, "asset_class", "fixed_income", parse_date("01/01/23")),
    (2, "interest_rate", 199, parse_date("01/01/23")),
    (1, "gics_industry", "biotechnology", parse_date("01/01/24")),
    (2, "gics_sector", "technology", parse_date("01/01/23")),
    (1, "asset_class", "equity", parse_date("01/01/24")),
    (1, "name", "Graphite bio", parse_date("01/01/24")),
    (2, "gics_sector", "financials", parse_date("03/17/23")),
    (1, "market_cap", 400, parse_date("05/23/24")),
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

We'll start at the high-level, there isn't much to see yet:

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

In the case that there is already a row in the table for the security in question, we want to instead copy this previous row, as our new facts will instead represent diffs to this **prior information**. There is a nice unity to the API here in both of these cases, in that once we have the prior row regardless if it's from the same security or a new one, we can call `diff_row` with our facts to correctly update the tuple. We can also observe the importance of our column index used to index into the tuple attributes.

Once we've successfully diff'ed the tuple, we also need to update the `effective_end_date` of the prior row if it was from the same security. That row's validity ends on the effective date of the new row we're adding. After we've done that, we add the new row to the table accumulator and return.

#### Diffing the Rows in The Table

```python
def diff_row(prior_row: tuple, col_index: ColumnIndex, flat_fact: FlatFactSet):
    new_row = list(prior_row)
    security_id, effective_date, kv_pairs = flat_fact

    # Set id & date range
    (
        new_row[col_index["security_id"]],
        new_row[col_index["effective_start_date"]],
    ) = (security_id, effective_date)

    # Add new kv_pairs that diff from prior row
    for key, value in kv_pairs:
        new_row[col_index[key]] = value

    return tuple(new_row)
```

Inside `diff_row` we take the prior row, convert it to a mutable list, set the `security_id` and `effective_start_date` to their respective values, and finall loop over the attribute pairs and set those as well. We convert it back to a tuple and return.

Putting it all together we have:

```python
def generate_security_master(
    audit_trail: AuditTrail, attribute_priority: dict[str, int]
) -> SecurityMaster:
    header, col_index = extract_header(audit_trail, attribute_priority)
    sorted_flat_facts = generate_sorted_flat_facts(audit_trail)
    data = generate_data_from_facts(sorted_flat_facts, col_index)

    return SecurityMaster.from_tuple((header, data, col_index))
```

which returns our Security Master object. The Security Master object is a simple Pydantic object defined as:

```python
class SecurityMaster(BaseModel):
    header: Header
    data: SMData
    col_index: ColumnIndex

    @classmethod
    def from_tuple(cls, t: tuple[Header, SMData, ColumnIndex]):
        return cls(header=t[0], data=t[1], col_index=t[2])

    def to_tuple(self):
        return (self.header, self.data, self.col_index)
```

### Output

If we print the table by running `python -m hedgineer`, we should see something like this (it might look messed up if the screen space is too narrow):

```
Security Master
Security Master
security_id     effective_start_date    effective_end_date      asset_class     ticker  name                    gics_industry   gics_sector     interest_rate   market_cap
1               01/01/24                03/22/24                equity          GRPH    Graphite bio            biotechnology   healthcare      None            None
1               03/22/24                05/23/24                equity          LENZ    Lenz Therapeutics, Inc  biotechnology   healthcare      None            None
1               05/23/24                None                    equity          LENZ    Lenz Therapeutics, Inc  biotechnology   healthcare      None            400
2               01/01/23                03/17/23                fixed_income    V       None                    None            technology      199             None
2               03/17/23                05/23/24                fixed_income    V       None                    None            financials      199             None
2               05/23/24                None                    fixed_income    V       None                    None            financials      199             549000
3               01/01/24                None                    None            ACME    None                    None            None            None            None
```

which is a type-2 SCD dimension table for each unique security generated from the audit trail [shown above](#audit-trail). Note that the facts present in the audit table are essentially shuffled, highlighting that this transformation is independent of the order that facts are received. We note that on March 22, 2024 the firm "Graphite bio" changed its name to "Lenz Therapeutics, Inc", along with an accompanying change to its ticker. This change is reflected in the second row, and importantly, the historical data is maintained. The market cap attribute was similarly updated on May 23, 2024.

## Joining on Positions

Once we have our Security Master, it's relatively straightforward to generate derived data. An example of this is security positions. Assume we are given a position table of the following format:

```
security_id    quantity    date
1              100         02/01/24
1              105         02/01/24
2              150         02/01/24
1              120         03/01/24
2              140         03/01/24
```

We can join this data with our Security Master in the following way:

```python
def join_positions(sm: SecurityMaster, positions_table: list[tuple]) -> JoinedPositions:
    attributes = [
        attr
        for attr in sm.header
        if attr not in ["security_id", "effective_start_date", "effective_end_date"]
    ]
    header = ["security_id", "quantity", "date", *attributes]
    joined_positions = list(
        filter(
            lambda x: x != (),
            [join_position(sm, attributes, position) for position in positions_table],
        )
    )

    return JoinedPositions.from_tuple((header, joined_positions))
```

We first extract our header information by filtering out `effective_start_date` and `effective_end_date` and adding in `quantity` and `date` which we will take from the positions table. We then collect the collated rows via the `join_positions` function and collect this data and the header into a Pydantic object as before.

Let's examine `join_position`:

```python
def join_position(sm: SecurityMaster, attributes: list[str], position: tuple) -> tuple:
    security_id, quantity, date = position

    try:
        master_row = next(
            filter(
                lambda x: x[sm.col_index["security_id"]] == security_id
                and x[sm.col_index["effective_start_date"]] <= date
                and (
                    x[sm.col_index["effective_end_date"]] is None
                    or x[sm.col_index["effective_end_date"]] > date
                ),
                sm.data,
            )
        )
    except StopIteration:
        return tuple([])

    return tuple(
        (
            security_id,
            quantity,
            date,
            *map(lambda attr: master_row[sm.col_index[attr]], attributes),
        )
    )
```

Here, we extract the row yielded by a filter object restricted to rows in the Security Master that have the same `security_id` and that contain the `date` field from the row in the position table. Given the nature of our Security Master, this should only be one or zero rows. If the row is available we collate the attributes along with the position date, otherwise we return an empty tuple. We can then display this in the console similarly as before with the SM.

Running `python -m hedgineer -m -p` should yield:

```
Consolidated Position Information
security_id     quantity        date            asset_class     ticker  name            gics_industry   gics_sector     interest_rate   market_cap
1               100             02/01/24        equity          GRPH    Graphite bio    biotechnology   healthcare      None            None
1               105             02/01/24        equity          GRPH    Graphite bio    biotechnology   healthcare      None            None
2               150             02/01/24        fixed_income    V       None            None            financials      199             None
1               120             03/01/24        equity          GRPH    Graphite bio    biotechnology   healthcare      None            None
2               140             03/01/24        fixed_income    V       None            None            financials      199             None
3               100             03/01/24        None            ACME    None            None            None            None            None
```

## Enhancement A: Slicing & Dicing by Asset Class

Our Security Master can contain many different types of securities. Because different asset classes have different characteristics, this manifests itself in many columns that are only present for some asset classes. For instance, equities generally don't have an interest rate. This is only one example.

As suggested in the email prompt, we might want to view separate tables for each asset class without extraneous columns.

I have to confess I very nearly wildly overcomplicated this. I was thinking about having separate tables and adding another heirarchy to our key structure, so `(asset_class, security_id, effective_date)`. This wasn't very elegant and would mean that we would need to move rows between tables when asset class facts were received in our audit trail.

Luckily before implementing this I realized that we already have all the data we need in the Security Master table; all we want is a different **view** of this data. This is a much more elegant solution of course. I was very pleased that I checked myself before going down the wrong path.

The implementation is relatively straightforward, we simply filter out those rows that are not in the supplied `asset_class`:

```python
def filter_by_asset_class(sm: SecurityMaster, asset_class: str | None):
    filtered_sm = SecurityMaster.from_tuple(
        (
            sm.header,
            list(
                filter(lambda x: x[sm.col_index["asset_class"]] == asset_class, sm.data)
            ),
            sm.col_index,
        )
    )
    return remove_empty_columns(filtered_sm)
```

and then remove empty columns. `remove_empty_columns` is relatively tedious and mechanical so for brevity's sake I omit it here.

Running `python -m hedgineer -f fixed_income` yields:

```
Security Master (asset_class: fixed_income)
security_id     effective_start_date    effective_end_date      asset_class     ticker  gics_sector     interest_rate   market_cap
2               01/01/23                03/17/23                fixed_income    V       technology      199             None
2               03/17/23                05/23/24                fixed_income    V       financials      199             None
2               05/23/24                None                    fixed_income    V       financials      199             549000
```

## Enhancement B: Merging in New Data

So that's not a bad start. But the world is not static and we need to respond to new facts as we recive them. We could restart the pipeline from scratch with an updated audit trail, but that might not be feasible for performance reasons. What we really need is a way to aggregate batches of updates from new facts and **merge** them into the existing Security Master. We can conceptualize this as inserting new rows into the table at their correct location.

There is a lot of nuance here. First, where do new rows in the table go? Well according to their hierarchical index, I was able to find five cases:

1. The fact is the first piece of information for security, i.e. we need to insert a completely new `security_id` into the table ("Insert New")
2. The fact is a piece of information for an existing security, and has an `effective_date` after all current entries in the table ("Insert After")
3. The fact is a piece of information for an existing security, and has an `effective_date` before all current entries in the table ("Insert Before")
4. The fact is a piece of information for an existing security, and has an `effective_date` the same as some entry in the table ("Merge")
5. The fact is a piece of information for an existing security, and has an `effective_date` after some entry in the table but before the next corresponding entry (for the same security) in the table ("Split")

Cases 3, 4, and 5 have the additional complication that value changes at these points in the table have downstream changes to subsequent rows: that is to say, attribute values need to **propogate** to later time periods until they were overwritten (interestingly enough by data that the SM incorporated before!). I call this "cascading".

An example is illustrative. Say we have the following Security Master - on March 22, 2024 "Graphite bio" changed their name to "Lenz Therapeutics, Inc" along with their exchange ticker.

```
security_id     effective_start_date    effective_end_date  ticker  name                     gics_industry
1               01/01/24                03/22/24            GRPH    Graphite bio             None
1               03/22/24                None                LENZ    Lenz Therapeutics, Inc   None
```

Suppose that we receive the fact `(1, "03/01/24", [("gics_industry", "technology")])` and wish to merge this into the table. Since March 1st is after January 1st but before March 22nd, we need to split these rows and insert a new row in the middle to reflect this new information. Importantly, the new attribute value is **cascaded** down to subsequent rows.

```
security_id     effective_start_date    effective_end_date  ticker  name                     gics_industry
1               01/01/24                03/01/24            GRPH    Graphite bio             None
1               03/01/24                03/22/24            GRPH    Graphite bio             technology
1               03/22/24                None                LENZ    Lenz Therapeutics, Inc   technology
```

If we suppose that our initial table instead looked like the following, i.e. that as part of the rebrand Graphite bio updated their `gics_industry` code:

```
security_id     effective_start_date    effective_end_date  ticker  name                     gics_industry
1               01/01/24                03/22/24            GRPH    Graphite bio             None
1               03/22/24                None                LENZ    Lenz Therapeutics, Inc   health sciences
```

Then the corresponding merged table would look like the following:

```
security_id     effective_start_date    effective_end_date  ticker  name                     gics_industry
1               01/01/24                03/01/24            GRPH    Graphite bio             None
1               03/01/24                03/22/24            GRPH    Graphite bio             technology
1               03/22/24                None                LENZ    Lenz Therapeutics, Inc   health sciences
```

because the technology value for this attribute gets "overwritten" by future fact changes.

#### New Attributes

There is also the sticky case of new attributes in our update batch that are not present in our original Security Master. This manifests itself in a new column in the table. Every other row gets set to a default value of `None` and the new column is added. Relatively straightforward, but something to keep in mind in our implementation.

### Merge Implementation

The implementation for merging a new audit trail in the Security Master is contained in the below snippet:

```python
def merge_audit_trail_update(
    sm: SecurityMaster,
    audit_trail_update: AuditTrail,
    attribute_priority: dict[str, int],
):
    sm = expand_attributes(
        sm,
        audit_trail_update,
        attribute_priority,
    )

    for flat_fact in generate_sorted_flat_facts(audit_trail_update):
        sm = merge_flat_fact(
            sm,
            flat_fact,
        )

    return sm
```

The `expand_attributes` function takes in the Security Master and adds the new columns with `None` values, if there are any. As with some other portions of the code, this is relatively mechanical and tedious, so I omit details here.

As before we bucket, flatten, and sort the facts from the audit trail update and then merge them into the Security Master, returning the updated instance each time. `merge_flat_fact` takes in a new fact, determines the insertion index, creates the corresponding row and inserts it, and finally **cascades** the new values down as we discussed above:

```python
def merge_flat_fact(
    sm: SecurityMaster,
    flat_fact: FlatFactSet,
) -> SecurityMaster:
    security_id, d, _ = flat_fact
    security_rows = list(filter(lambda x: x[0] == security_id, sm.data))

    if len(security_rows) == 0:
        # "Insert New" case
    elif d < security_rows[0][1]:
        # "Insert Before" case
    elif d > security_rows[-1][1]:
        # "Insert After" case
    elif any(map(lambda x: x[1] == d, security_rows)):
        # "Merge" case
    else:
        row_to_split = next(row for row in security_rows if row[1] <= d < row[2])
        # "Split" case

    ...

    return sm
```

Running `python -m hedgineer -m` yields:

```
Security Master
security_id     effective_start_date    effective_end_date      asset_class     ticker  name                    gics_industry   gics_sector     interest_rate   market_cap
1               01/01/24                03/22/24                equity          GRPH    Graphite bio            biotechnology   healthcare      None            None
1               03/22/24                05/23/24                equity          LENZ    Lenz Therapeutics, Inc  biotechnology   healthcare      None            None
1               05/23/24                None                    equity          LENZ    Lenz Therapeutics, Inc  biotechnology   healthcare      None            400
2               01/01/23                03/17/23                fixed_income    V       None                    None            technology      199             None
2               03/17/23                05/23/24                fixed_income    V       None                    None            financials      199             None
2               05/23/24                None                    fixed_income    V       None                    None            financials      199             549000
3               01/01/24                None                    None            ACME    None                    None            None            None            None

Security Master after Merge
security_id     effective_start_date    effective_end_date      asset_class     ticker  name                    gics_industry   gics_sector     interest_rate   market_cap      new_key
1               01/01/24                03/01/24                equity          GRPH    Graphite bio            biotechnology   healthcare      None            None            None
1               03/01/24                03/22/24                equity          GRPH    Graphite bio            health sciences healthcare      None            100             None
1               03/22/24                05/23/24                equity          LENZ    Lenz Therapeutics, Inc  health sciences healthcare      None            100             None
1               05/23/24                05/26/24                equity          LENZ    Lenz Therapeutics, Inc  health sciences healthcare      None            400             None
1               05/26/24                None                    equity          LENZ    Lenz Therapeutics, Inc  health sciences healthcare      None            90000           123
2               01/01/23                03/17/23                fixed_income    V       None                    None            technology      199             None            None
2               03/17/23                05/23/24                fixed_income    V       None                    None            financials      199             None            456
2               05/23/24                05/26/24                fixed_income    V       None                    None            financials      199             549000          456
2               05/26/24                None                    fixed_income    V       None                    None            financials      199             548000          456
3               01/01/24                None                    None            ACME    None                    None            None            None            None            None
```

which shows the original table plus an updated table merged with data from an updated audit trail:

```
AUDIT_TRAIL_UPDATE: list[tuple] = [
    (1, "market_cap", 100, parse_date("03/01/24")),
    (1, "gics_industry", "health sciences", parse_date("03/01/24")),
    (1, "market_cap", 90000, parse_date("05/26/24")),
    (2, "market_cap", 548000, parse_date("05/26/24")),
    (1, "new_key", 123, parse_date("05/26/24")),
    (2, "new_key", 456, parse_date("03/17/23")),
]
```

#### Streaming

Note that we can define batch updates by their granularity or size, i.e. how quickly and with what size of batch. As the batch size approaches 1 and we merge in new facts as we receive them, at some point we can start to say that the merging process is no longer performed in "batches" but is actually a "streaming" API that merges changes in real-time.

## Enhancement C: Robustly Exporting to External Tools

We might want to export our Security Master to other tools, programming languages, environments, databases etc. We also might want to persist copies of it for durability or compliance requirements. I wanted to explore how I might use Apache Arrow to serve as a unified interface for exporting and importing. Arrow is an in-memory format for describing structued tabular data in a column-first format. If we can serialize our Security Master data to Arrow, we can use Arrow as an interface for exporting to various other formats and importing this data back to our Security Master in exactly the format we expect.

This turned out to work really well, and I was super pleased how this little experiment turned out! Using the official Python bindings `pyarrow`, it was very straightforward to serialize our Security Master to an in-memory Arrow table:

```python
import pyarrow as pa

def parse_data_type(column):
    # Some exception handling and special cases
    # ...
    # ...

    column_type = ...

    if column_type is int:
        return pa.int64()
    elif column_type is float:
        return pa.float64()
    elif column_type is str:
        return pa.string()
    elif column_type is date:
        return pa.date32()

    # ...

def to_arrow(sm: SecurityMaster) -> tuple[pa.Table, pa.Schema]:
    raw_columns = list(zip(*sm.data))
    data_types = map(parse_data_type, raw_columns)
    schema = pa.schema(list(zip(sm.header, data_types)))

    return pa.table(raw_columns, schema=schema), schema
```

Here `pa.table(raw_columns, schema=schema)` is the in-memory Arrow table and `schema` is the schema object which can be useful for deserializing later on.
Reading from Arrow is similarly very straightforward (we don't need the schema in this case as Arrow maintains that information in it's own metadata):

```python
def from_arrow(arrow_table) -> SecurityMaster:
    py_table = arrow_table.to_pylist()
    header = tuple(k for k in py_table[0].keys()) if len(py_table) > 0 else tuple()
    col_index = {v: i for i, v in enumerate(header)}
    data = [tuple(v for v in row.values()) for row in py_table]

    return SecurityMaster.from_tuple((list(header), data, col_index))
```

#### Conversions

Once we have an in-memory Arrow table, it's super straightforward to convert to Pandas:

```python
def to_pandas(sm: SecurityMaster):
    arrow_table, schema = to_arrow(sm)
    return arrow_table.to_pandas(), schema


def from_pandas(df, schema):
    arrow_table = pa.Table.from_pandas(df, schema=schema)
    return from_arrow(arrow_table)
```

or to Parquet:

```python
def write_parquet(sm, where):
    arrow_table, schema = to_arrow(sm)
    pq.write_table(arrow_table, where)

    return schema, where


def read_parquet(where, schema):
    arrow_table = pq.read_table(where, schema=schema)
    return from_arrow(arrow_table)
```

or to CSV:

```python
def write_csv(sm, where):
    arrow_table, schema = to_arrow(sm)
    convert_options = csv.ConvertOptions(
        column_types={field.name: field.type for field in schema},
        strings_can_be_null=True,
    )

    csv.write_csv(arrow_table, where)
    return convert_options, where


def read_csv(where, convert_options):
    arrow_table = csv.read_csv(where, convert_options=convert_options)
    return from_arrow(arrow_table)
```

or even to SQL!:

```python
def write_sql(
    sm: SecurityMaster,
    engine,
    metadata,
    table_name: str,
):
    arrow_table, schema = to_arrow(sm)
    columns = list(map(map_field_to_sql_column, schema))
    sql_table = Table(table_name, metadata, *columns)

    with engine.connect() as conn:
        conn.execute(CreateTable(sql_table, if_not_exists=True))
        conn.execute(insert(sql_table).values(arrow_table.to_pylist()))
        conn.commit()

    return schema, metadata


def read_sql(schema, engine, metadata, table_name: str):
    table = metadata.tables[table_name]
    with engine.connect() as conn:
        rows = list(conn.execute(select(table)))

    arrow_table, schema = to_arrow_with_schema(rows, schema)
    return from_arrow(arrow_table)
```

If you run `python -m hedgineer -s` you should see an in-memory echo representation of the SQL queries that would be used to store and retrieve the Security Master from a SQL Database.

## Addendum: Enhancement D?: Python testing Other Implementations

I didn't have time to try and implement a small demo of this, but the smooth nature of exporting to Apache Arrow gave me a really interesting idea. Let's say hypothetically we're building parts of our data management pipeline in Python. We're definitely going to have some tests that test our exported data and our pipeline's functionality. Now, we could write these tests purely against Python native data structures. However if we _instead_ run these tests against these Apache Arrow arrays, a really interesting possibility opens up.

Let's say at some point we start to scale and performance and reliability become an issue, and we decide to rewrite parts of our platform in a separate language (Java, Go, Rust etc.). Because our tests are written against a sharable, convertible format, we can slowly rewrite portions of the platform that demand performance improvements, but we get to keep our tests in python to verify our new implementation. That's awesome!
