package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

type FuncInfo struct {
	Name     string
	QualName string
	Short    string
	File     string
	Line     int
	Doc      string
}

var (
	allFuncs = map[string]*FuncInfo{}
	edgeSet  = map[string]map[string]bool{}

	knownReceivers = map[string]bool{
		"c": true, "cs": true, "srv": true, "cc": true,
	}
	knownPackages = map[string]bool{
		"message": true, "logger": true,
	}
	fileColors = map[string]string{
		"main.go":             "#4361ee",
		"serve.go":            "#2ec4b6",
		"cp.go":               "#e76f51",
		"cp_to_host.go":       "#f4a261",
		"cp_from_host.go":     "#e76f51",
		"cp_dir.go":           "#d62828",
		"cp_from_host_dir.go": "#d62828",
		"client.go":           "#9b5de5",
		"transfer.go":         "#f15bb5",
		"glob.go":             "#e9c46a",
		"progressbar.go":      "#f15bb5",
		"logger.go":           "#6c757d",
		"message.go":          "#9b5de5",
		"implement.go":        "#9b5de5",
		"interface.go":        "#9b5de5",
	}
)

func main() {
	out := flag.String("output", "gcp_call_chain.svg", "output SVG path")
	flag.Parse()

	fset := token.NewFileSet()

	dirs := []string{"cmd/gcp", "message", "cmd/progressbar", "logger"}
	for _, dir := range dirs {
		filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			parseFile(path, fset)
			return nil
		})
	}

	addEdge("main", "serveCmd.Action")
	addEdge("main", "cpCmd.Action")

	// gnet event loop calls these callbacks — no AST edge exists.
	addEdge("serveCmd.Action", "OnTraffic")
	// Channel dispatch from OnTraffic → process() worker goroutine.
	// The goroutine is spawned inside (*copierServer).process() at line 74.
	addEdge("OnTraffic", "anon_serve.go_74")

	reachable := bfs("main")

	dotPath := strings.TrimSuffix(*out, ".svg") + ".dot"
	dot := genDOT(reachable)
	if err := os.WriteFile(dotPath, []byte(dot), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing dot: %v\n", err)
		os.Exit(1)
	}

	cmd := exec.Command("dot", "-Tsvg", "-o", *out, dotPath)
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "error running dot: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Wrote %s\n", *out)
}

func addEdge(from, to string) {
	if edgeSet[from] == nil {
		edgeSet[from] = map[string]bool{}
	}
	edgeSet[from][to] = true
}

func registerFunc(name string, info *FuncInfo) {
	allFuncs[name] = info
}

func parseFile(path string, fset *token.FileSet) {
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse error %s: %v\n", path, err)
		return
	}

	var nodeStack []ast.Node
	var funcStack []string

	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			if len(nodeStack) > 0 {
				last := nodeStack[len(nodeStack)-1]
				if _, ok := last.(*ast.FuncDecl); ok && len(funcStack) > 0 {
					funcStack = funcStack[:len(funcStack)-1]
				} else if _, ok := last.(*ast.FuncLit); ok && len(funcStack) > 0 {
					funcStack = funcStack[:len(funcStack)-1]
				}
				nodeStack = nodeStack[:len(nodeStack)-1]
			}
			return true
		}

		nodeStack = append(nodeStack, n)

		switch node := n.(type) {
		case *ast.FuncDecl:
			qn, dn := funcDeclName(node)
			pos := fset.Position(node.Pos())
			info := &FuncInfo{
				Name: dn, QualName: qn, Short: node.Name.Name,
				File: path, Line: pos.Line, Doc: firstDocLine(node.Doc),
			}
			// Always register under the short name (function/method name).
			// This is what resolveCallTarget returns, so edges will match.
			registerFunc(node.Name.Name, info)
			// Also register under the qualified name for completeness.
			if qn != node.Name.Name {
				registerFunc(qn, info)
			}
			// Use the short name on the func stack so caller→caller matching
			// is consistent with resolveCallTarget.
			funcStack = append(funcStack, node.Name.Name)

		case *ast.FuncLit:
			name := anonFuncName(node, nodeStack, fset, path)
			pos := fset.Position(node.Pos())
			registerFunc(name, &FuncInfo{
				Name: name, QualName: name,
				File: path, Line: pos.Line,
			})
			funcStack = append(funcStack, name)

		case *ast.CallExpr:
			if len(funcStack) == 0 {
				break
			}
			caller := funcStack[len(funcStack)-1]
			// Named call: resolve target and add edge unconditionally.
			// The target might not be registered yet (different file not yet
			// parsed), but the name will be in allFuncs by graph-gen time.
			if target := resolveCallTarget(node.Fun); target != "" {
				addEdge(caller, target)
			} else if fl, ok := node.Fun.(*ast.FuncLit); ok {
				// Immediately-invoked function literal (e.g. go func(){...}()).
				// The FuncLit is a child of this CallExpr in the AST and will
				// be visited next, registering under a deterministic name.
				pos := fset.Position(fl.Pos())
				key := fmt.Sprintf("anon_%s_%d", filepath.Base(path), pos.Line)
				addEdge(caller, key)
			}
		}

		return true
	})
}

func funcDeclName(fd *ast.FuncDecl) (qualName, displayName string) {
	name := fd.Name.Name
	if fd.Recv != nil && len(fd.Recv.List) > 0 {
		rt := recvTypeName(fd.Recv.List[0].Type)
		qualName = rt + "." + name
		displayName = qualName
	} else {
		qualName = name
		displayName = name
	}
	return
}

func recvTypeName(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return "*" + recvTypeName(e.X)
	case *ast.Ident:
		return e.Name
	default:
		return fmt.Sprintf("%T", e)
	}
}

func anonFuncName(fl *ast.FuncLit, stack []ast.Node, fset *token.FileSet, path string) string {
	var varName, keyName, assignName string
	inCallExpr := false

	for i := len(stack) - 2; i >= 0; i-- {
		switch n := stack[i].(type) {
		case *ast.ValueSpec:
			if len(n.Names) > 0 {
				varName = n.Names[0].Name
			}
		case *ast.KeyValueExpr:
			if ident, ok := n.Key.(*ast.Ident); ok {
				keyName = ident.Name
			}
		case *ast.AssignStmt:
			if len(n.Lhs) > 0 {
				if ident, ok := n.Lhs[0].(*ast.Ident); ok {
					assignName = ident.Name
				}
			}
		case *ast.CallExpr:
			inCallExpr = true
		case *ast.FuncDecl, *ast.FuncLit:
			break
		}
		if varName != "" {
			break
		}
	}

	if varName != "" && keyName != "" {
		return varName + "." + keyName
	}
	if assignName != "" && keyName != "" {
		return assignName + "." + keyName
	}
	if varName != "" {
		return varName
	}

	pos := fset.Position(fl.Pos())
	if inCallExpr {
		return fmt.Sprintf("anon_%s_%d", filepath.Base(path), pos.Line)
	}
	return fmt.Sprintf("anon_%s_%d", filepath.Base(path), pos.Line)
}

func resolveCallTarget(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.Ident:
		return f.Name
	case *ast.SelectorExpr:
		if f.Sel == nil {
			return ""
		}
		if ident, ok := f.X.(*ast.Ident); ok {
			if knownPackages[ident.Name] {
				return f.Sel.Name
			}
			if knownReceivers[ident.Name] {
				return f.Sel.Name
			}
		}
		return ""
	case *ast.ParenExpr:
		return resolveCallTarget(f.X)
	case *ast.StarExpr:
		return resolveCallTarget(f.X)
	default:
		return ""
	}
}

func firstDocLine(g *ast.CommentGroup) string {
	if g == nil {
		return ""
	}
	for _, c := range g.List {
		line := strings.TrimLeft(c.Text, "/* ")
		if line != "" {
			return line
		}
	}
	return ""
}

func bfs(root string) map[string]bool {
	reachable := map[string]bool{}
	queue := []string{root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if reachable[cur] {
			continue
		}
		reachable[cur] = true
		for next := range edgeSet[cur] {
			if allFuncs[next] != nil && !reachable[next] {
				queue = append(queue, next)
			}
		}
	}
	return reachable
}

func colorForFile(path string) string {
	base := filepath.Base(path)
	if c, ok := fileColors[base]; ok {
		return c
	}
	return "#6c757d"
}

func relFile(path string) string {
	if idx := strings.Index(path, "cmd/"); idx >= 0 {
		return path[idx:]
	}
	if idx := strings.Index(path, "message/"); idx >= 0 {
		return path[idx:]
	}
	if idx := strings.Index(path, "logger/"); idx >= 0 {
		return path[idx:]
	}
	return path
}

func genDOT(reachable map[string]bool) string {
	var b strings.Builder
	b.WriteString("digraph gcp {\n")
	b.WriteString("  rankdir=TB;\n")
	b.WriteString("  ranksep=0.8;\n")
	b.WriteString("  nodesep=0.5;\n")
	b.WriteString("  node [shape=box, style=\"rounded,filled\", fontcolor=white, fontname=\"system-ui,sans-serif\", fontsize=11];\n")
	b.WriteString("  edge [color=\"#555555\", penwidth=2];\n")
	b.WriteString("  splines=true;\n\n")

	names := make([]string, 0, len(allFuncs))
	for n := range allFuncs {
		if reachable[n] {
			names = append(names, n)
		}
	}
	sort.Strings(names)

	for _, name := range names {
		info := allFuncs[name]
		display := info.Name
		if len(display) > 40 {
			display = display[:37] + "..."
		}
		color := colorForFile(info.File)
		label := fmt.Sprintf("%s\\n%s:%d", display, relFile(info.File), info.Line)
		b.WriteString(fmt.Sprintf("  %q [label=%q, fillcolor=%q];\n", name, label, color))
	}

	b.WriteString("\n")
	for from, targets := range edgeSet {
		if !reachable[from] {
			continue
		}
		targetList := make([]string, 0, len(targets))
		for t := range targets {
			if reachable[t] {
				targetList = append(targetList, t)
			}
		}
		sort.Strings(targetList)
		for _, to := range targetList {
			b.WriteString(fmt.Sprintf("  %q -> %q;\n", from, to))
		}
	}

	b.WriteString("}\n")
	return b.String()
}
