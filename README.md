dac
===

An data access component. It provides a library of classes and a tool, using it in .net project can make data accessing more easily.


Sample Code:
===

Initialize the DataContext:

var dc = new DataContext("DefaultConnectionString");
or
var dc = new DataContext("server=localhost;User Id=root;password=root;database=test;");


Lambda expression:

var q = this.dc.Query<Item>(i => i.Name == "test" && i.Status == "ok");
or
var q = from s in dc.GetQuery<Supplier>() where s.SuppId > 0 && s.Name == "for Test3" select s;


Insert, Delete, Update:

this.dc.Insert<Product>(new Product{ ProductId = Guid.NewGuid().ToString(), CategoryId = categoryId, Name = "for Test4" });
this.dc.Delete<Supplier>(suppId);
this.dc.Update<Product>(product);
this.dc.Save<Category>(category);


Get entity by key:

var product = this.dc.GetEntity<Product>(productId);


Ordering and Paging:

List<Item> items = this.dc.Query<Item>(ExpressionExtension.Empty<Item>().Take(3)).ToList();
var q = (from i in this.dc.GetQuery<Item>() select i).Skip(1).Take(2);
int count = this.dc.Query<Item>(ExpressionExtension.Empty<Item>().Skip(1)).Count();


Partial columns:

this.dc.Save<Item>(item, new string[] { "UnitCost" });
this.dc.Insert<Item>(item, new Expression<Func<Item, object>>[] {  i => i.ItemId, i => i.Name, i => i.ListPrice, i => i.ProductId, i => i.Status});


No entity query:

Command command = new Command("SELECT * FROM Product WHERE CategoryId = @p1");
command.AddParameter("@p1", categoryId);
var q = this.dc.Query(command);
int count = 0;
foreach (dynamic p in q)
{     
  Assert.IsNotNull(p);     
  Assert.AreEqual(p.CategoryId, categoryId);     
  count++;
}


Transaction:

this.dc.BeginTransaction();
this.dc.CommitTransaction();
this.dc.RollbackTransaction();
