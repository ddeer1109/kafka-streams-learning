# Java Streams API Patterns

tags: java, streams, functional

Common patterns for Java Stream API:

```java
// Collect to map with merge function (handle duplicate keys)
map.entrySet().stream()
    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> b));

// Partition by predicate (true/false groups)
Map<Boolean, List<Item>> partitioned = items.stream()
    .collect(Collectors.partitioningBy(Item::isActive));

// flatMap for nested collections
List<String> allTags = notes.stream()
    .flatMap(note -> note.getTags().stream())
    .distinct()
    .collect(Collectors.toList());
```
