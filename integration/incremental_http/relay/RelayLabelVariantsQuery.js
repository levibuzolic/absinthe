import { graphql } from "relay-runtime";

export const query = graphql`
  query RelayLabelVariantsQuery {
    first: person {
      ...RelayLabelVariantsQuery_details
    }
    second: person {
      ...RelayLabelVariantsQuery_details @arguments(wait: true)
    }
  }
`;

export const details = graphql`
  fragment RelayLabelVariantsQuery_details on Person
  @argumentDefinitions(wait: { type: "Boolean!", defaultValue: false }) {
    friend(wait: $wait) {
      id
    }
    ...RelayLabelVariantsQuery_name @defer
  }
`;

export const name = graphql`
  fragment RelayLabelVariantsQuery_name on Person {
    name
  }
`;
