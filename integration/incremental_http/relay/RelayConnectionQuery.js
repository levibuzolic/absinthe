import { graphql } from "relay-runtime";

export const query = graphql`
  query RelayConnectionQuery($count: Int!, $cursor: String, $initial: Int!) {
    peopleConnection(first: $count, after: $cursor)
      @stream_connection(
        key: "RelayConnectionQuery__peopleConnection"
        initial_count: $initial
      ) {
      edges {
        cursor
        node {
          id
          __typename
          name
        }
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        startCursor
        endCursor
      }
    }
  }
`;

export const nullable = graphql`
  query RelayConnectionQueryNullEdgeQuery(
    $count: Int!
    $cursor: String
    $initial: Int!
  ) {
    nullablePeopleConnection(first: $count, after: $cursor)
      @stream_connection(
        key: "RelayConnectionQueryNullEdgeQuery__nullablePeopleConnection"
        initial_count: $initial
      ) {
      edges {
        cursor
        node {
          id
          __typename
          name
        }
      }
      pageInfo {
        hasNextPage
        hasPreviousPage
        startCursor
        endCursor
      }
    }
  }
`;
