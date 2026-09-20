import { graphql } from "relay-runtime";

export const mutation = graphql`
  mutation RelayMutationQuery {
    first {
      id
      delayedId
      ...RelayMutationQuery_details @defer
    }
    second {
      id
    }
  }
`;

export const details = graphql`
  fragment RelayMutationQuery_details on Person {
    name
  }
`;
