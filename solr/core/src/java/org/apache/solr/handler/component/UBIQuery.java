package org.apache.solr.handler.component;

/**
 * Handles all the data required for tracking a query using User Behavior Insights.
 *
 * <p>Compatible with the
 * https://github.com/o19s/ubi/blob/main/schema/X.Y.Z/query.request.schema.json.
 */
public class UBIQuery {

  private String queryId;
  private String userQuery;
  private Object queryAttributes;

  public UBIQuery(String queryId) {

    if (queryId == null) {
      queryId = "1234";
    }
    this.queryId = queryId;
  }

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public String getUserQuery() {
    return userQuery;
  }

  public void setUserQuery(String userQuery) {
    this.userQuery = userQuery;
  }

  public Object getQueryAttributes() {
    return queryAttributes;
  }

  public void setQueryAttributes(Object queryAttributes) {
    this.queryAttributes = queryAttributes;
  }

  /**
   * Convert the UBIQuery into the format consumed by a streaming expression tuple()
   *
   * @return String The tuple specific formatted data similar to "query_id=123,user_query=foo"
   */
  public String toTuple() {
    return UBIComponent.QUERY_ID
        + "="
        + this.queryId
        + ","
        + UBIComponent.USER_QUERY
        + "="
        + this.userQuery;
  }
}
