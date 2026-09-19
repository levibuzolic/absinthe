import { graphql } from "relay-runtime";

export const stream = graphql`
  query RelayCasesStreamQuery($initial: Int!, $enabled: Boolean!) {
    roster: people
      @stream(initialCount: $initial, if: $enabled, label: "roster") {
      id
      __typename
      name
    }
  }
`;

export const nested = graphql`
  query RelayCasesNestedQuery($initial: Int!) {
    hero: person {
      id
      ...RelayCases_friends @defer
    }
  }
`;
export const friends = graphql`
  fragment RelayCases_friends on Person {
    crew: friends @stream(initialCount: $initial, label: "crew") {
      id
      __typename
      ...RelayCases_name @defer
    }
  }
`;
export const name = graphql`
  fragment RelayCases_name on Person {
    name
    ...RelayCases_age @defer
  }
`;
export const age = graphql`
  fragment RelayCases_age on Person {
    age
  }
`;

export const shared = graphql`
  query RelayCasesSharedQuery {
    person {
      id
      ...RelayCases_a @defer
      ...RelayCases_b @defer
    }
  }
`;
export const a = graphql`
  fragment RelayCases_a on Person {
    name
    age
  }
`;
export const b = graphql`
  fragment RelayCases_b on Person {
    name
    friend {
      id
      name
    }
  }
`;

export const ancestor = graphql`
  query RelayCasesAncestorQuery {
    person {
      id
      age
      ...RelayCases_outer @defer
    }
  }
`;
export const outer = graphql`
  fragment RelayCases_outer on Person {
    age
    ...RelayCases_inner @defer
  }
`;
export const inner = graphql`
  fragment RelayCases_inner on Person {
    name
  }
`;

export const lateWork = graphql`
  query RelayCasesLateWorkQuery {
    person {
      id
      ...RelayCases_lateOuter @defer
    }
  }
`;
export const lateOuter = graphql`
  fragment RelayCases_lateOuter on Person {
    ...RelayCases_lateInner @defer
    friend {
      id
      ...RelayCases_lateOuterLeaf @defer
    }
  }
`;
export const lateInner = graphql`
  fragment RelayCases_lateInner on Person {
    friend {
      age
      ...RelayCases_lateInnerLeaf @defer
    }
  }
`;
export const lateInnerLeaf = graphql`
  fragment RelayCases_lateInnerLeaf on Person {
    name
    id
  }
`;
export const lateOuterLeaf = graphql`
  fragment RelayCases_lateOuterLeaf on Person {
    name
    id
  }
`;

export const failure = graphql`
  query RelayCasesFailureQuery {
    person {
      id
      ...RelayCases_failure @defer
    }
  }
`;
export const failureDetails = graphql`
  fragment RelayCases_failure on Person {
    name
    failure
  }
`;
export const fatal = graphql`
  query RelayCasesFatalQuery {
    person {
      id
      ...RelayCases_fatal @defer
      ...RelayCases_slow @defer
    }
  }
`;
export const fatalDetails = graphql`
  fragment RelayCases_fatal on Person {
    requiredFailure
  }
`;
export const slow = graphql`
  query RelayCasesSlowQuery {
    person {
      id
      ...RelayCases_slow @defer
    }
  }
`;
export const slowDetails = graphql`
  fragment RelayCases_slow on Person {
    slow
  }
`;

export const nestedStreams = graphql`
  query RelayCasesNestedStreamsQuery {
    people @stream(initialCount: 0) {
      id
      friends @stream(initialCount: 0) {
        id
        name
      }
    }
  }
`;

export const ancestorPath = graphql`
  query RelayCasesAncestorPathQuery {
    person {
      id
      friends {
        id
      }
      ...RelayCases_outerPath @defer
    }
  }
`;
export const outerPath = graphql`
  fragment RelayCases_outerPath on Person {
    friends {
      id
      ...RelayCases_inner @defer
    }
  }
`;
export const nullItem = graphql`
  query RelayCasesNullItemQuery {
    nullablePeople @stream(initialCount: 0) {
      id
      name
    }
  }
`;

export const streamErrors = graphql`
  query RelayCasesStreamErrorsQuery {
    people @stream(initialCount: 0) {
      id
      failure
    }
  }
`;

export const streamFatal = graphql`
  query RelayCasesStreamFatalQuery {
    requiredPeople @stream(initialCount: 1) {
      id
      name
    }
  }
`;

export const nullError = graphql`
  query RelayCasesNullErrorQuery {
    people @stream(initialCount: 0) {
      id
      requiredFailure
    }
  }
`;
export const nullNested = graphql`
  query RelayCasesNullNestedQuery {
    nullablePeople @stream(initialCount: 0) {
      id
      ...RelayCases_name @defer
    }
  }
`;

export const abstract = graphql`
  query RelayCasesAbstractQuery {
    entity: node {
      id
      __typename
      ...RelayCases_nodeA @defer
      ...RelayCases_nodeB @defer
    }
  }
`;
export const nodeA = graphql`
  fragment RelayCases_nodeA on Node {
    id
    ... on Person {
      name
    }
  }
`;
export const nodeB = graphql`
  fragment RelayCases_nodeB on Node {
    id
    ... on Person {
      name
      age
    }
  }
`;

export const projection = graphql`
  query RelayCasesProjectionQuery {
    entity: person {
      id
      __typename
      ...RelayCases_projectionCommon
      ...RelayCases_projectionDetails @defer
    }
  }
`;
export const projectionCommon = graphql`
  fragment RelayCases_projectionCommon on Node {
    id
  }
`;
export const projectionDetails = graphql`
  fragment RelayCases_projectionDetails on Person {
    ...RelayCases_projectionCommon
    age
  }
`;
