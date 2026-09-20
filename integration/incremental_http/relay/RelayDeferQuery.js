import { graphql } from "relay-runtime";

export const query = graphql`
  query RelayDeferQuery {
    hero: person {
      id
      __typename
      ...RelayDeferQuery_details @defer
    }
  }
`;

export const details = graphql`
  fragment RelayDeferQuery_details on Person {
    display: name
  }
`;
