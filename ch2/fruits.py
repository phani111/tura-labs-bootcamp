import apache_beam as beam

# print("Now with ParDo")

# class ExplodeByValue(beam.DoFn):
#     def process(self, element, *args, **kwargs):
#         for _ in range(element[1]):
#             yield dict(zip(['name', 'value'], element))


# with beam.Pipeline() as p:
#     rows = (
#         p 
#         | beam.Create([('apple', 1), ('banana', 3), ('cherry', 5)])
#         | beam.ParDo(ExplodeByValue())
#         | beam.Map(print)
#     )

print("\nGroupByKey + CombineValues")

with beam.Pipeline() as p:
    rows = (
        p
        | beam.Create([('apple', 1),
                       ('banana', 3),
                       ('cherry', 5),
                       ('apple', 6),
                       ('banana', 4),
                       ('apple', 4)])
        | beam.GroupByKey()
        | beam.CombineValues(sum)
        | beam.Map(print)
    )

print("\n IO w/ TupleCombineFnb example")
with beam.Pipeline() as p:
    fruits = (
        p
        | beam.io.ReadFromText('../deb/data/input/ch2/ep2/fruits.csv')
        | beam.Map(lambda line: str(line).strip().split(','))
        | beam.Map(lambda row: row[:2] + [int(row[2])])
        | beam.Map(lambda row: (tuple(row[:2]), tuple(row[2:] * 3)))
    )

    totals = (
        fruits
        | beam.CombinePerKey(
            beam.combiners.TupleCombineFn(
                beam.combiners.CountCombineFn(),
                sum,
                beam.combiners.MeanCombineFn())
    ))
        
    p = (totals | "print totals" >> beam.Map(print))

    p = (
        totals
        | beam.Map(lambda key_value_tuple: key_value_tuple[0] + key_value_tuple[1])
        | beam.Map(lambda flat_tuple: ','.join([str(_) for _ in flat_tuple]))
        | beam.io.WriteToText('fruit_totals', file_name_suffix=".txt")
    )