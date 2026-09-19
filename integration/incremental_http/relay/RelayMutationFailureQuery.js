import { graphql } from "relay-runtime";

export const mutation = graphql`
  mutation RelayMutationFailureQuery {
    first {
      id
      ...RelayMutationFailureQuery_failure @defer
    }
    second {
      id
      ...RelayMutationFailureQuery_later @defer
    }
  }
`;

export const failure = graphql`
  fragment RelayMutationFailureQuery_failure on Person {
    requiredFailure
  }
`;

export const later = graphql`
  fragment RelayMutationFailureQuery_later on Person {
    name
  }
`;
