def setup_month_nums():
    """
    Returns a list of alternating English month representations and the two-digit
    month number corresponding to them, for use in populating the month_nums
    dictionary.
    """
    return [
        ('Jan', '01'), ('Jan.', '01'), ('January', '01'),
        ('Feb', '02'), ('Feb.', '02'), ('February', '02'),
        ('Mar', '03'), ('Mar.', '03'), ('March', '03'),
        ('Apr', '04'), ('Apr.', '04'), ('April', '04'),
        ('May', '05'),
        ('Jun', '06'), ('Jun.', '06'), ('June', '06'),
        ('Jul', '07'), ('Jul.', '07'), ('July', '07'),
        ('Aug', '08'), ('Aug.', '08'), ('August', '08'),
        ('Sep', '09'), ('Sep.', '09'),
        ('Sept', '09'), ('Sept.', '09'), ('September', '09'),
        ('Oct', '10'), ('Oct.', '10'), ('October', '10'),
        ('Nov', '11'), ('Nov.', '11'), ('November', '11'),
        ('Dec', '12'), ('Dec.', '12'), ('December', '12')
    ]

def setup_lucene_bugzilla_jira_map():
    """
    Returns a list of alternating Bugzilla bug IDs and LUCENE-* JIRA issue
     numbers, for use in populating the %lucene_bugzilla_jira_map hash
    """
    return {
        4049: 1, 4102: 2, 4105: 3, 4254: 4, 4555: 5, 4568: 6, 4754: 7, 5313: 8,
        36296: 428, 36333: 429, 36622: 430, 36623: 431, 36628: 432
    }
